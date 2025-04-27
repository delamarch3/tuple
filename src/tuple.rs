use std::borrow::Cow;
use std::cmp::Ordering;
use std::io::{self, Read, Write};

use bytes::{BufMut, BytesMut};

use crate::schema::{PhysicalAttrs, Schema, Type};
use crate::value::Value;

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
    /// TODO: remove this in favour of [`Self::get_by_physical_attrs()`]
    pub fn get<'a>(&'a self, schema: &Schema, position: usize) -> Value<'a> {
        let r#type = schema.get_type(position);

        let Some(bytes) = self.get_bytes(schema, position) else {
            return Value::Null;
        };

        match r#type {
            Type::String => Value::String(Cow::Borrowed(bytes)),
            Type::Int8 => {
                let value = i8::from_be_bytes(bytes.try_into().unwrap());
                Value::Int8(value)
            }
            Type::Int32 => {
                let value = i32::from_be_bytes(bytes.try_into().unwrap());
                Value::Int32(value)
            }
            Type::Float32 => {
                let value = f32::from_be_bytes(bytes.try_into().unwrap());
                Value::Float32(value)
            }
        }
    }

    /// Gets the bytes of the ith column of the schema. Note that the nullability of the column in
    /// the schema is ignored, null is returned based on the tuples null bitmap only.
    fn get_bytes<'a>(&'a self, schema: &Schema, position: usize) -> Option<&'a [u8]> {
        let PhysicalAttrs { r#type, offset, .. } = schema.get_physical_attrs(position);

        let i = position / 64;
        let bit = position % 64;
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

    /// Gets the value of the ith column of the schema. Note that the nullability of the column in
    /// the schema is ignored, null is returned based on the tuples null bitmap only.
    #[inline]
    pub fn get_by_physical_attrs<'a>(
        &'a self,
        PhysicalAttrs {
            r#type,
            position,
            offset,
        }: PhysicalAttrs,
    ) -> Value<'a> {
        let i = position / 8;
        let bit = position % 8;
        if self.nulls[i] & (1 << bit) > 0 {
            return Value::Null;
        }

        match r#type {
            Type::String => {
                let length =
                    u16::from_be_bytes(self.data[offset + 2..offset + 4].try_into().unwrap())
                        as usize;
                let offset =
                    u16::from_be_bytes(self.data[offset..offset + 2].try_into().unwrap()) as usize;
                Value::String(Cow::Borrowed(&self.data[offset..offset + length]))
            }
            Type::Int8 => {
                let value = i8::from_be_bytes(
                    self.data[offset..offset + r#type.size()]
                        .try_into()
                        .unwrap(),
                );
                Value::Int8(value)
            }
            Type::Int32 => {
                let value = i32::from_be_bytes(
                    self.data[offset..offset + r#type.size()]
                        .try_into()
                        .unwrap(),
                );
                Value::Int32(value)
            }
            Type::Float32 => {
                let value = f32::from_be_bytes(
                    self.data[offset..offset + r#type.size()]
                        .try_into()
                        .unwrap(),
                );
                Value::Float32(value)
            }
        }
    }

    /// Creates a new tuple which is greater than the current one by the first value.
    pub fn next(&self, schema: &Schema) -> Self {
        use Value::*;

        // TODO: handle overflow
        let first = match self.get(schema, 0) {
            String(value) if value.is_empty() => String(Cow::Owned(vec![0])),
            String(mut value) => 'string: {
                for b in value.to_mut().iter_mut() {
                    if *b < 255 {
                        *b += 1;
                        break 'string String(value);
                    }
                }

                value.to_mut().push(0);
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

    pub fn write_to(&self, w: &mut impl Write) -> io::Result<()> {
        w.write_all(self.null_bytes())?;
        w.write_all(self.data_bytes())?;

        Ok(())
    }

    pub fn from_bytes(bytes: &[u8], schema: &Schema) -> Tuple {
        let nulls_size = schema.nulls_size();
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

    pub fn read_from(r: &mut impl Read, schema: &Schema) -> io::Result<Tuple> {
        let nulls_size = schema.nulls_size();
        let data_size = schema.size();

        let mut nulls = vec![0; nulls_size];
        r.read_exact(&mut nulls)?;

        let mut data = BytesMut::zeroed(data_size);
        r.read_exact(&mut data)?;

        let strings_size = schema.string_pointer_offsets().fold(0, |acc, offset| {
            let length = u16::from_be_bytes(data[offset + 2..offset + 4].try_into().unwrap());
            acc + length
        }) as usize;

        let mut strings = vec![0; strings_size];
        r.read_exact(&mut strings)?;

        data.extend(strings);

        Ok(Self { nulls, data })
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
        let nulls_len = schema.nulls_size();
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

    #[allow(clippy::should_implement_trait)]
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
    use std::borrow::Cow;
    use std::cmp::Ordering::*;
    use std::io::{Cursor, Read};

    use super::{Tuple, TupleBuilder};
    use crate::schema::{Schema, Type};
    use crate::value::Value::*;

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
            String(Cow::Borrowed(
                b"the quick brown fox jumps over the lazy dog",
            )),
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
            tuple.write_to(&mut c).unwrap();
        });

        c.set_position(0);
        tuples.iter().for_each(|want| {
            let have = Tuple::read_from(&mut c, &schema).unwrap();
            assert_eq!(want.cmp(&have, &schema), Equal);
        });

        c.set_position(0);
        tuples.iter().for_each(|want| {
            let mut buf = vec![0; want.size()];
            c.read_exact(&mut buf).unwrap();
            let have = Tuple::from_bytes(&buf, &schema);
            assert_eq!(want.cmp(&have, &schema), Equal);
        });

        Ok(())
    }
}
