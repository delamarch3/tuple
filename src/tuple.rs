use std::borrow::Cow;
use std::cmp::Ordering;
use std::io::{self, Read, Write};

use bytes::{BufMut, BytesMut};

use crate::schema::{OffsetIter, Type};
use crate::value::Value;

/// The layout of the tuple is:
/// u8 length | [u8] types | \[Value] data | [u8] variable section
///
/// In the data section, a string is represented as a fat pointer to the variable length section (a
/// 16bit offset and 16bit length). When a tuple is being constructed, the strings are collected
/// separately instead of being directly appended to the tuple because their offsets will only be
/// known once the variable length section is appended.
///
/// The length of the columns is represented as the first byte in the tuple. Consequently, there is
/// a column limit of 255. The types are also encoded as bytes. The types are iterated over to
/// determine the offsets for each of the values.
#[derive(PartialEq)]
pub struct Tuple {
    types: Vec<Type>,
    offsets: Vec<usize>,
    data: BytesMut,
}

impl std::fmt::Debug for Tuple {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut sep = "";
        (0..self.types.len())
            .map(|position| self.get(position))
            .try_for_each(|value| {
                write!(f, "{sep}")?;
                write!(f, "{:?}", value)?;
                Ok(sep = "|")
            })
    }
}

impl Tuple {
    pub fn type_bytes(&self) -> &[u8] {
        // Safety: `Type` is `u8`
        unsafe { std::slice::from_raw_parts(self.types.as_ptr() as *const u8, self.types.len()) }
    }

    pub fn data_bytes(&self) -> &[u8] {
        &self.data[..]
    }

    pub fn size(&self) -> usize {
        1 + self.types.len() + self.data.len()
    }

    /// Gets the value of the ith column of the tuple.
    #[inline]
    pub fn get<'a>(&'a self, position: usize) -> Value<'a> {
        let r#type = self.types[position];
        let offset = self.offsets[position];

        match r#type {
            Type::Null => Value::Null,
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
    pub fn next(&self) -> Self {
        // TODO: handle overflow
        let first = match self.get(0) {
            Value::String(value) if value.is_empty() => Value::String(Cow::Owned(vec![0])),
            Value::String(mut value) => 'string: {
                for b in value.to_mut().iter_mut() {
                    if *b < 255 {
                        *b += 1;
                        break 'string Value::String(value);
                    }
                }

                value.to_mut().push(0);
                Value::String(value)
            }
            Value::Int8(value) => Value::Int8(value + 1),
            Value::Int32(value) => Value::Int32(value + 1),
            Value::Float32(value) => Value::Float32(value + f32::EPSILON),
            Value::Null => unimplemented!("creating a next tuple for one with null columns"),
        };

        (1..self.types.len())
            .fold(TupleBuilder::new().add(first), |b, position| {
                b.add(self.get(position))
            })
            .finish()
    }

    /// Compares `self` and `other` by iterating and comparing each column of the tuple.
    /// TODO: implement [`PartialOrd`] now that schema is not required
    pub fn cmp(&self, other: &Tuple) -> Ordering {
        use Ordering::*;

        debug_assert_eq!(self.types.len(), other.types.len());

        for position in 0..self.types.len() {
            let lhs = self.get(position);
            let rhs = other.get(position);

            match lhs.cmp(&rhs) {
                ord @ Less | ord @ Greater => return ord,
                Equal => continue,
            }
        }

        Equal
    }

    pub fn write_to(&self, w: &mut impl Write) -> io::Result<()> {
        w.write_all(std::slice::from_ref(&(self.types.len() as u8)))?;
        w.write_all(self.type_bytes())?;
        w.write_all(self.data_bytes())?;

        Ok(())
    }

    pub fn from_bytes(mut bytes: &[u8]) -> Tuple {
        let types_size = bytes[0] as usize;
        bytes = &bytes[1..];
        let types = &bytes[0..types_size];
        // Safety: `u8` is `Type`
        let types =
            unsafe { std::slice::from_raw_parts(types.as_ptr() as *const Type, types.len()) }
                .to_vec();
        let offsets: Vec<_> = OffsetIter::new(types.iter()).collect();

        let data_size = types.iter().fold(0, |acc, x| acc + x.size());
        let data = &bytes[types_size..types_size + data_size];

        let strings_size = types
            .iter()
            .enumerate()
            .filter(|(_, r#type)| matches!(r#type, Type::String))
            .map(|(position, _)| offsets[position])
            .fold(0, |acc, offset| {
                let length =
                    u16::from_be_bytes(data[offset + 2..offset + 4].try_into().unwrap()) as usize;
                acc + length
            });

        let data = BytesMut::from(&bytes[types_size..types_size + data_size + strings_size]);

        Self {
            types,
            offsets,
            data,
        }
    }

    pub fn read_from(r: &mut impl Read) -> io::Result<Tuple> {
        let mut types_size = [0; 1];
        r.read_exact(&mut types_size)?;

        let mut types = vec![0; types_size[0].into()];
        r.read_exact(&mut types)?;
        // Safety: `u8` is `Type`
        let types =
            unsafe { std::slice::from_raw_parts(types.as_ptr() as *const Type, types.len()) }
                .to_vec();
        let offsets: Vec<_> = OffsetIter::new(types.iter()).collect();

        let data_size = types.iter().fold(0, |acc, x| acc + x.size());
        let mut data = BytesMut::zeroed(data_size);
        r.read_exact(&mut data)?;

        let strings_size = types
            .iter()
            .enumerate()
            .filter(|(_, r#type)| matches!(r#type, Type::String))
            .map(|(position, _)| offsets[position])
            .fold(0, |acc, offset| {
                let length =
                    u16::from_be_bytes(data[offset + 2..offset + 4].try_into().unwrap()) as usize;
                acc + length
            });

        let mut strings = vec![0; strings_size];
        r.read_exact(&mut strings)?;

        data.extend(strings);

        Ok(Self {
            types,
            offsets,
            data,
        })
    }
}

/// Creates a new tuple using only the columns in the schema. The schema offsets are expected to
/// align with the offsets in the tuple. This is useful for creating composite key tuple out of
/// non-contiguous columns.
pub fn fit_tuple_with_schema(tuple: &Tuple, positions: impl Iterator<Item = usize>) -> Tuple {
    let mut builder = TupleBuilder::new();
    for position in positions {
        let value = tuple.get(position);
        builder = builder.add(value);
    }

    builder.finish()
}

struct Variable {
    data: Vec<u8>,
    /// The offset of the pointer within the data section
    pointer_offset: usize,
}

pub struct TupleBuilder {
    types: Vec<Type>,
    data: BytesMut,
    vars: Vec<Variable>,
}

impl TupleBuilder {
    pub fn new() -> Self {
        let types = Vec::new();
        let data = BytesMut::new();
        let vars = Vec::new();

        Self { types, data, vars }
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
        self.types.push(Type::String);

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
        self.types.push(Type::Int8);
        self.data.put_i8(value);
        self
    }

    pub fn int32(mut self, value: i32) -> Self {
        self.types.push(Type::Int32);
        self.data.put_i32(value);
        self
    }

    pub fn float32(mut self, value: f32) -> Self {
        self.types.push(Type::Float32);
        self.data.put_f32(value);
        self
    }

    pub fn null(mut self) -> Self {
        self.types.push(Type::Null);
        self
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

        let offsets: Vec<_> = OffsetIter::new(self.types.iter()).collect();

        Tuple {
            types: self.types,
            offsets,
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

        let tuple = TupleBuilder::new()
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
        .zip(schema.positions())
        .for_each(|(want, position)| assert_eq!(tuple.get(position), want));
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
            TupleBuilder::new()
                .int8(32)
                .string(b"the quick brown fox jumps over the lazy dog")
                .null()
                .int32(7)
                .finish(),
            TupleBuilder::new().null().null().null().null().finish(),
        ]
        .into_iter()
        .for_each(|want| {
            let builder = TupleBuilder::new();
            let have = schema
                .positions()
                .fold(builder, |b, position| b.add(want.get(position)))
                .finish();

            assert_eq!(want, have);
        });
    }

    #[test]
    fn test_next() {
        let tuple = TupleBuilder::new().int8(0).finish();
        let have = tuple.next();
        let want = TupleBuilder::new().int8(1).finish();
        assert_eq!(want, have);

        let tuple = TupleBuilder::new().float32(0.).finish();
        let have = tuple.next();
        let want = TupleBuilder::new().float32(0. + f32::EPSILON).finish();
        assert_eq!(want, have);

        let tuple = TupleBuilder::new().string(&[0]).finish();
        let have = tuple.next();
        let want = TupleBuilder::new().string(&[1]).finish();
        assert_eq!(want, have);

        let tuple = TupleBuilder::new().string(&[255]).finish();
        let have = tuple.next();
        let want = TupleBuilder::new().string(&[255, 0]).finish();
        assert_eq!(want, have);
    }

    #[test]
    fn test_cmp() {
        let tuples = [
            TupleBuilder::new()
                .int8(32)
                .string(b"abc")
                .null()
                .int32(7)
                .finish(),
            TupleBuilder::new()
                .int8(32)
                .string(b"abd")
                .null()
                .int32(7)
                .finish(),
            TupleBuilder::new()
                .int8(32)
                .string(b"abc")
                .null()
                .null()
                .finish(),
        ];

        [(0, 0, Equal), (0, 1, Less), (0, 2, Greater)]
            .into_iter()
            .for_each(|(lhs, rhs, want)| {
                let have = tuples[lhs].cmp(&tuples[rhs]);
                assert_eq!(want, have);
            });
    }

    #[test]
    fn test_write_read_roundtrip() -> std::io::Result<()> {
        let tuples = [
            TupleBuilder::new()
                .int8(32)
                .string(b"the quick brown fox jumps over the lazy dog")
                .null()
                .int32(7)
                .finish(),
            TupleBuilder::new()
                .int8(32)
                .string(b"abd")
                .float32(16.)
                .int32(7)
                .finish(),
            TupleBuilder::new()
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
            let have = Tuple::read_from(&mut c).unwrap();
            assert_eq!(want.cmp(&have), Equal);
        });

        c.set_position(0);
        tuples.iter().for_each(|want| {
            let mut buf = vec![0; want.size()];
            c.read_exact(&mut buf).unwrap();
            let have = Tuple::from_bytes(&buf);
            assert_eq!(want.cmp(&have), Equal);
        });

        Ok(())
    }
}
