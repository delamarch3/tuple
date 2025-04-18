pub mod schema;

use bytes::{BufMut, BytesMut};

use crate::schema::{Schema, Type};

#[derive(Debug, PartialEq)]
pub enum Value {
    String(Vec<u8>),
    Int8(i8),
    Int32(i32),
    Float32(f32),
    Null,
}

/// The layout of the tuple is:
/// variable null bitmap | data (eg int, float, string pointer) | variable section
///
/// In the data section, a string is represented as a fat pointer to the variable length section, eg
/// 2 byte offset, 2 byte length. When a tuple is being constructed, the strings are collected
/// separately instead of being directly appended to the tuple because their offsets will only be
/// known once the variable length section is appended.
///
/// The size of the null bitmap is determined by the number of columns in the schema. Each section
/// of the bitmap will be a u64. So if there are 65 columns, the size of the null bitmap will be
/// 128 bits. For simplicity the null section will exist even if there are no nullable columns in the
/// schema, unless the schema is empty, in that case the null section is also empty.
/// Also for simplicity, the size of the null value will still be appended to the data
/// section, which will ease reading the tuple, since the offsets of all values update once a null
/// is introduced.
/// TODO: consider encoding the typeids in the tuple - do not need to hold offsets in the schema
/// and can store null easily. Then get value would look like: check types len, iterate until pos
/// while keeping track of the offset. if typeid = 0 return null, else read offset and use type
/// size. We can build the typeid list once in the schema and assert after build that they match.
/// For [`fit_tuple_with_schema()`], we could use the indices of the columns instead of offsets from
/// schema.
pub struct Tuple {
    nulls: Vec<u64>,
    data: BytesMut,
}

impl Tuple {
    /// Gets the value of the ith column of the schema. Note that the nullability of the column in
    /// the schema is ignored, null is returned based on the tuples null bitmap only.
    fn get(&self, schema: &Schema, pos: usize) -> Value {
        use Value::*;

        let (r#type, offset) = schema.get_physical_attrs(pos);

        let i = pos / 64;
        let bit = pos % 64;
        if self.nulls[i] & (1 << bit) > 0 {
            return Null;
        }

        match r#type {
            Type::String => {
                let length =
                    u16::from_be_bytes(self.data[offset + 2..offset + 4].try_into().unwrap())
                        as usize;
                let offset =
                    u16::from_be_bytes(self.data[offset..offset + 2].try_into().unwrap()) as usize;
                let value = &self.data[offset..offset + length];
                String(value.to_vec())
            }
            Type::Int8 => {
                let value = i8::from_be_bytes(self.data[offset..offset + 1].try_into().unwrap());
                Int8(value)
            }
            Type::Int32 => {
                let value = i32::from_be_bytes(self.data[offset..offset + 4].try_into().unwrap());
                Int32(value)
            }
            Type::Float32 => {
                let value = f32::from_be_bytes(self.data[offset..offset + 4].try_into().unwrap());
                Float32(value)
            }
        }
    }

    /// Creates a new tuple which is greater than the current one by the first value.
    pub fn next(&self, schema: &Schema) -> Self {
        use Value::*;

        // TODO: handle overflow
        let first = match self.get(schema, 0) {
            String(_) => todo!(),
            Int8(value) => Int8(value + 1),
            Int32(value) => Int32(value + 1),
            Float32(value) => Float32(value + f32::EPSILON),
            Null => todo!(), // This would need to turn into a non-null value by consulting the schema
        };

        let mut builder = TupleBuilder::new(schema);
        builder = builder.add(first);
        builder = (0..schema.len()).fold(builder, |b, i| b.add(self.get(schema, i)));
        builder.finish()
    }
}

/// Creates a new tuple using only the columns in the schema. The schema offsets are expected to
/// align with the offsets in the tuple. This is useful for creating composite key tuple out of
/// non-contiguous columns.
pub fn fit_tuple_with_schema(tuple: &Tuple, schema: &Schema) -> Tuple {
    // TODO: Probably do not need to pass the entire schema to the tuple builder
    let mut compact_schema = schema.clone();
    compact_schema.compact();
    let mut builder = TupleBuilder::new(&compact_schema);
    for position in schema.positions() {
        let value = tuple.get(schema, position);
        builder = builder.add(value);
    }

    builder.finish()
}

struct Variable {
    data: Vec<u8>,
    /// The offset of the pointer within the data section
    pointer_offset: usize,
}

pub struct TupleBuilder<'a> {
    schema: &'a Schema,
    data: BytesMut,
    vars: Vec<Variable>,
    nulls: Vec<u64>,
    position: usize,
}

impl<'a> TupleBuilder<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        let nulls_len = (schema.columns().len() / 64) + 1;
        let data_size = schema.size();

        let mut nulls = Vec::new();
        (0..nulls_len).for_each(|_| nulls.push(0));

        let data = BytesMut::with_capacity(data_size);
        let vars = Vec::new();
        let position = 0;

        Self {
            schema,
            data,
            vars,
            nulls,
            position,
        }
    }

    pub fn add(self, value: Value) -> Self {
        match value {
            Value::String(value) => self.string(&value),
            Value::Int8(value) => self.int8(value),
            Value::Int32(value) => self.int32(value),
            Value::Float32(value) => self.float32(value),
            Value::Null => self.null(),
        }
    }

    pub fn string(mut self, value: &[u8]) -> Self {
        self.position += 1;

        let pointer_offset = self.data.len();
        self.data.put_u16(0); // Offset
        self.data.put_u16(u16::try_from(value.len()).unwrap()); // Length

        self.vars.push(Variable {
            data: value.to_vec(),
            pointer_offset,
        });

        self
    }

    pub fn int8(mut self, value: i8) -> Self {
        self.position += 1;
        self.data.put_i8(value);
        self
    }

    pub fn int32(mut self, value: i32) -> Self {
        self.position += 1;
        self.data.put_i32(value);
        self
    }

    pub fn float32(mut self, value: f32) -> Self {
        self.position += 1;
        self.data.put_f32(value);
        self
    }

    pub fn null(mut self) -> Self {
        self.position += 1;

        let pos = self.position - 1;
        let i = pos / 64;
        let bit = pos % 64;
        self.nulls[i] |= 1 << bit;

        match self.schema.get_type(pos).unwrap() {
            Type::String => self.string(&[]),
            Type::Int8 => self.int8(0),
            Type::Int32 => self.int32(0),
            Type::Float32 => self.float32(0.),
        }
    }

    pub fn finish(mut self) -> Tuple {
        for Variable {
            data,
            pointer_offset,
        } in self.vars
        {
            let offset = self.data.len();
            self.data.put(data.as_slice());
            self.data[pointer_offset..pointer_offset + 2]
                .copy_from_slice(&(offset as u16).to_be_bytes());
        }

        Tuple {
            nulls: self.nulls,
            data: self.data,
        }
    }
}

#[cfg(test)]
mod test {
    use super::{TupleBuilder, Value::*};
    use crate::schema::{Schema, Type};

    #[test]
    fn test_build_and_get() {
        let nullable = true;
        let schema = Schema::default()
            .add_column("c1".into(), Type::Int8, nullable)
            .add_column("c2".into(), Type::String, nullable)
            .add_column("c3".into(), Type::Float32, nullable)
            .add_column("c4".into(), Type::Int32, nullable);

        let tuple = TupleBuilder::new(&schema)
            .int8(32)
            .string(b"the quick brown fox jumps over the lazy dog")
            .null()
            .int32(7)
            .finish();

        [
            Int8(32),
            String(b"the quick brown fox jumps over the lazy dog".to_vec()),
            Null,
            Int32(7),
        ]
        .into_iter()
        .enumerate()
        .for_each(|(i, want)| assert_eq!(tuple.get(&schema, i), want));
    }
}
