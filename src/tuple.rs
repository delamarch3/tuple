use std::cmp::Ordering;

use bytes::{BufMut, BytesMut};

use crate::schema::{Schema, Type};

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum Value {
    String(Vec<u8>),
    Int8(i8),
    Int32(i32),
    Float32(f32),
    Null,
}

impl Eq for Value {}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        use Ordering::*;

        match self {
            Value::String(lhs) => match other {
                Value::String(rhs) => lhs.cmp(rhs),
                Value::Null => Greater,
                _ => panic!("unsupported comparison"),
            },
            Value::Int8(lhs) => match other {
                Value::Int8(rhs) => lhs.cmp(rhs),
                Value::Null => Greater,
                _ => panic!("unsupported comparison"),
            },
            Value::Int32(lhs) => match other {
                Value::Int32(rhs) => lhs.cmp(rhs),
                Value::Null => Greater,
                _ => panic!("unsupported comparison"),
            },
            Value::Float32(lhs) => match other {
                Value::Float32(rhs) => lhs.total_cmp(rhs),
                Value::Null => Greater,
                _ => panic!("unsupported comparison"),
            },
            Value::Null => match other {
                Value::Null => Equal,
                _ => Less,
            },
        }
    }
}

impl std::ops::Add for Value {
    type Output = Value;

    fn add(self, rhs: Self) -> Self::Output {
        match self {
            Value::String(mut lhs) => match rhs {
                Value::String(rhs) => {
                    lhs.extend(rhs);
                    Value::String(lhs)
                }
                _ => panic!("unsupported operation"),
            },
            Value::Int8(lhs) => match rhs {
                Value::Int8(rhs) => Value::Int8(lhs + rhs),
                Value::Null => self,
                _ => panic!("unsupported operation"),
            },
            Value::Int32(lhs) => match rhs {
                Value::Int32(rhs) => Value::Int32(lhs + rhs),
                Value::Null => self,
                _ => panic!("unsupported operation"),
            },
            Value::Float32(lhs) => match rhs {
                Value::Float32(rhs) => Value::Float32(lhs + rhs),
                Value::Null => self,
                _ => panic!("unsupported operation"),
            },
            Value::Null => Value::Null,
        }
    }
}

impl std::ops::Sub for Value {
    type Output = Value;

    fn sub(self, rhs: Self) -> Self::Output {
        match self {
            Value::String(_) => panic!("unsupported operation"),
            Value::Int8(lhs) => match rhs {
                Value::Int8(rhs) => Value::Int8(lhs - rhs),
                Value::Null => self,
                _ => panic!("unsupported operation"),
            },
            Value::Int32(lhs) => match rhs {
                Value::Int32(rhs) => Value::Int32(lhs - rhs),
                Value::Null => self,
                _ => panic!("unsupported operation"),
            },
            Value::Float32(lhs) => match rhs {
                Value::Float32(rhs) => Value::Float32(lhs - rhs),
                Value::Null => self,
                _ => panic!("unsupported operation"),
            },
            Value::Null => Value::Null,
        }
    }
}

impl std::ops::Mul for Value {
    type Output = Value;

    fn mul(self, rhs: Self) -> Self::Output {
        match self {
            Value::String(_) => panic!("unsupported operation"),
            Value::Int8(lhs) => match rhs {
                Value::Int8(rhs) => Value::Int8(lhs * rhs),
                Value::Null => self,
                _ => panic!("unsupported operation"),
            },
            Value::Int32(lhs) => match rhs {
                Value::Int32(rhs) => Value::Int32(lhs * rhs),
                Value::Null => self,
                _ => panic!("unsupported operation"),
            },
            Value::Float32(lhs) => match rhs {
                Value::Float32(rhs) => Value::Float32(lhs * rhs),
                Value::Null => self,
                _ => panic!("unsupported operation"),
            },
            Value::Null => Value::Null,
        }
    }
}

impl std::ops::Div for Value {
    type Output = Value;

    fn div(self, rhs: Self) -> Self::Output {
        match self {
            Value::String(_) => panic!("unsupported operation"),
            Value::Int8(lhs) => match rhs {
                Value::Int8(rhs) => Value::Int8(lhs / rhs),
                Value::Null => self,
                _ => panic!("unsupported operation"),
            },
            Value::Int32(lhs) => match rhs {
                Value::Int32(rhs) => Value::Int32(lhs / rhs),
                Value::Null => self,
                _ => panic!("unsupported operation"),
            },
            Value::Float32(lhs) => match rhs {
                Value::Float32(rhs) => Value::Float32(lhs / rhs),
                Value::Null => self,
                _ => panic!("unsupported operation"),
            },
            Value::Null => Value::Null,
        }
    }
}

impl Into<Value> for i8 {
    fn into(self) -> Value {
        Value::Int8(self)
    }
}

impl Into<Value> for i32 {
    fn into(self) -> Value {
        Value::Int32(self)
    }
}

impl Into<Value> for f32 {
    fn into(self) -> Value {
        Value::Float32(self)
    }
}

impl Into<Value> for String {
    fn into(self) -> Value {
        Value::String(self.into_bytes())
    }
}

impl Into<Value> for &str {
    fn into(self) -> Value {
        Value::String(self.as_bytes().to_vec())
    }
}

impl Value {
    pub fn is_zero(&self) -> bool {
        match self {
            Value::String(value) => value.is_empty(),
            Value::Int8(value) => *value == 0,
            Value::Int32(value) => *value == 0,
            Value::Float32(value) => *value == 0.,
            Value::Null => true,
        }
    }
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
/// of the bitmap will be a u8. So if there are 65 columns, the size of the null bitmap will be
/// 72 bits (9 bytes). For simplicity the null section will exist even if there are no nullable columns in the
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
#[derive(Debug, PartialEq)]
pub struct Tuple {
    nulls: Vec<u8>,
    data: BytesMut,
}

impl Tuple {
    pub fn null_bytes(&self) -> &[u8] {
        self.nulls.as_slice()
    }

    pub fn data_bytes(&self) -> &[u8] {
        &self.data[..]
    }

    pub fn size(&self) -> usize {
        self.null_bytes().len() + self.data_bytes().len()
    }

    /// Gets the value of the ith column of the schema. Note that the nullability of the column in
    /// the schema is ignored, null is returned based on the tuples null bitmap only.
    pub fn get(&self, schema: &Schema, pos: usize) -> Value {
        use Value::*;

        let r#type = schema.get_type(pos);

        let Some(bytes) = self.get_bytes(&schema, pos) else {
            return Null;
        };

        match r#type {
            Type::String => String(bytes.to_vec()),
            Type::Int8 => {
                let value = i8::from_be_bytes(bytes.try_into().unwrap());
                Int8(value)
            }
            Type::Int32 => {
                let value = i32::from_be_bytes(bytes.try_into().unwrap());
                Int32(value)
            }
            Type::Float32 => {
                let value = f32::from_be_bytes(bytes.try_into().unwrap());
                Float32(value)
            }
        }
    }

    /// Gets the bytes of the ith column of the schema. Note that the nullability of the column in
    /// the schema is ignored, null is returned based on the tuples null bitmap only.
    pub fn get_bytes<'a>(&'a self, schema: &Schema, pos: usize) -> Option<&'a [u8]> {
        let (r#type, offset) = schema.get_physical_attrs(pos);

        let i = pos / 8;
        let bit = pos % 8;
        if self.nulls[i] & (1 << bit) > 0 {
            return None;
        }

        match r#type {
            Type::String => {
                let length =
                    u16::from_be_bytes(self.data[offset + 2..offset + 4].try_into().unwrap())
                        as usize;
                let offset =
                    u16::from_be_bytes(self.data[offset..offset + 2].try_into().unwrap()) as usize;
                Some(&self.data[offset..offset + length])
            }
            _ => Some(&self.data[offset..offset + r#type.size()]),
        }
    }

    /// Creates a new tuple which is greater than the current one by the first value.
    pub fn next(&self, schema: &Schema) -> Self {
        use Value::*;

        // TODO: handle overflow
        let first = match self.get(schema, 0) {
            String(value) if value.len() == 0 => String(vec![0]),
            String(mut value) => 'string: {
                for b in value.iter_mut() {
                    if *b < 255 {
                        *b += 1;
                        break 'string String(value);
                    }
                }

                value.push(0);
                String(value)
            }
            Int8(value) => Int8(value + 1),
            Int32(value) => Int32(value + 1),
            Float32(value) => Float32(value + f32::EPSILON),
            Null => todo!(), // This would need to turn into a non-null value by consulting the schema
        };

        (1..schema.len())
            .fold(TupleBuilder::new(schema).add(first), |b, i| {
                b.add(self.get(schema, i))
            })
            .finish()
    }

    /// Compares `self` and `other` by iterating and comparing each column of the tuple.
    pub fn cmp(&self, other: &Tuple, schema: &Schema) -> Ordering {
        use Ordering::*;

        for pos in schema.positions() {
            let lhs = self.get(schema, pos);
            let rhs = other.get(schema, pos);

            match lhs.cmp(&rhs) {
                ord @ Less | ord @ Greater => return ord,
                Equal => continue,
            }
        }

        Equal
    }

    pub fn from_bytes(schema: &Schema, bytes: &[u8]) -> Tuple {
        let nulls_size = (schema.columns().len() / 8) + 1;
        let data_size = schema.size();

        let nulls = bytes[0..nulls_size].to_vec();
        let data = &bytes[nulls_size..nulls_size + data_size];
        let strings_size = schema.string_pointer_offsets().fold(0, |acc, offset| {
            let length = u16::from_be_bytes(data[offset + 2..offset + 4].try_into().unwrap());
            acc + length
        }) as usize;
        let data = BytesMut::from(&bytes[nulls_size..nulls_size + data_size + strings_size]);

        Self { nulls, data }
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
    nulls: Vec<u8>,
    position: usize,
}

impl<'a> TupleBuilder<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        let nulls_len = (schema.columns().len() / 8) + 1;
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
        let pos = self.position;
        let i = pos / 8;
        let bit = pos % 8;
        self.nulls[i] |= 1 << bit;

        match self.schema.get_type(pos) {
            Type::String => self.string(b""),
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
    use std::cmp::Ordering::*;
    use std::io::{Cursor, Read, Write};

    use super::{Tuple, TupleBuilder, Value::*};
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

    #[test]
    fn test_builder_roundtrip() {
        let nullable = true;
        let schema = Schema::default()
            .add_column("c1".into(), Type::Int8, nullable)
            .add_column("c2".into(), Type::String, nullable)
            .add_column("c3".into(), Type::Float32, nullable)
            .add_column("c4".into(), Type::Int32, nullable);

        [
            TupleBuilder::new(&schema)
                .int8(32)
                .string(b"the quick brown fox jumps over the lazy dog")
                .null()
                .int32(7)
                .finish(),
            TupleBuilder::new(&schema)
                .null()
                .null()
                .null()
                .null()
                .finish(),
        ]
        .into_iter()
        .for_each(|want| {
            let mut builder = TupleBuilder::new(&schema);
            builder = (0..schema.len()).fold(builder, |b, i| b.add(want.get(&schema, i)));
            let have = builder.finish();

            assert_eq!(want, have);
        });
    }

    #[test]
    fn test_next() {
        let nullable = true;

        let schema = Schema::default().add_column("c1".into(), Type::Int8, nullable);
        let tuple = TupleBuilder::new(&schema).int8(0).finish();
        let have = tuple.next(&schema);
        let want = TupleBuilder::new(&schema).int8(1).finish();
        assert_eq!(want, have);

        let schema = Schema::default().add_column("c1".into(), Type::Float32, nullable);
        let tuple = TupleBuilder::new(&schema).float32(0.).finish();
        let have = tuple.next(&schema);
        let want = TupleBuilder::new(&schema)
            .float32(0. + f32::EPSILON)
            .finish();
        assert_eq!(want, have);

        let schema = Schema::default().add_column("c1".into(), Type::String, nullable);
        let tuple = TupleBuilder::new(&schema).string(&[0]).finish();
        let have = tuple.next(&schema);
        let want = TupleBuilder::new(&schema).string(&[1]).finish();
        assert_eq!(want, have);

        let schema = Schema::default().add_column("c1".into(), Type::String, nullable);
        let tuple = TupleBuilder::new(&schema).string(&[255]).finish();
        let have = tuple.next(&schema);
        let want = TupleBuilder::new(&schema).string(&[255, 0]).finish();
        assert_eq!(want, have);
    }

    #[test]
    fn test_cmp() {
        let nullable = true;

        let schema = Schema::default()
            .add_column("c1".into(), Type::Int8, nullable)
            .add_column("c2".into(), Type::String, nullable)
            .add_column("c3".into(), Type::Float32, nullable)
            .add_column("c4".into(), Type::Int32, nullable);

        let tuples = [
            TupleBuilder::new(&schema)
                .int8(32)
                .string(b"abc")
                .null()
                .int32(7)
                .finish(),
            TupleBuilder::new(&schema)
                .int8(32)
                .string(b"abd")
                .null()
                .int32(7)
                .finish(),
            TupleBuilder::new(&schema)
                .int8(32)
                .string(b"abc")
                .null()
                .null()
                .finish(),
        ];

        [(0, 0, Equal), (0, 1, Less), (0, 2, Greater)]
            .into_iter()
            .for_each(|(lhs, rhs, want)| {
                let have = tuples[lhs].cmp(&tuples[rhs], &schema);
                assert_eq!(want, have);
            });
    }

    #[test]
    fn test_write_read_roundtrip() -> std::io::Result<()> {
        let nullable = true;

        let schema = Schema::default()
            .add_column("c1".into(), Type::Int8, nullable)
            .add_column("c2".into(), Type::String, nullable)
            .add_column("c3".into(), Type::Float32, nullable)
            .add_column("c4".into(), Type::Int32, nullable);

        let tuples = [
            TupleBuilder::new(&schema)
                .int8(32)
                .string(b"the quick brown fox jumps over the lazy dog")
                .null()
                .int32(7)
                .finish(),
            TupleBuilder::new(&schema)
                .int8(32)
                .string(b"abd")
                .float32(16.)
                .int32(7)
                .finish(),
            TupleBuilder::new(&schema)
                .int8(32)
                .null()
                .float32(7.2)
                .null()
                .finish(),
        ];

        let mut c = Cursor::new(Vec::new());
        tuples.iter().for_each(|tuple| {
            c.write(tuple.null_bytes()).unwrap();
            c.write(tuple.data_bytes()).unwrap();
        });

        c.set_position(0);
        tuples.iter().for_each(|want| {
            let mut buf = vec![0; want.size()];
            c.read_exact(&mut buf).unwrap();
            let have = Tuple::from_bytes(&schema, &buf);
            assert_eq!(want.cmp(&have, &schema), Equal);
        });

        Ok(())
    }
}
