use std::borrow::Cow;
use std::cmp::Ordering;

#[derive(Debug, PartialEq, Clone)]
pub enum Value<'a> {
    String(Cow<'a, [u8]>),
    Int8(i8),
    Int32(i32),
    Float32(f32),
    Null,
}

impl<'a> Eq for Value<'a> {}

impl<'a> PartialOrd for Value<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for Value<'a> {
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

impl<'a> std::ops::Add for Value<'a> {
    type Output = Value<'a>;

    fn add(self, rhs: Self) -> Self::Output {
        match self {
            Value::String(mut lhs) => match rhs {
                Value::String(rhs) => {
                    lhs.to_mut().extend(rhs.iter());
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

impl<'a> std::ops::Sub for Value<'a> {
    type Output = Value<'a>;

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

impl<'a> std::ops::Mul for Value<'a> {
    type Output = Value<'a>;

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

impl<'a> std::ops::Div for Value<'a> {
    type Output = Value<'a>;

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

impl<'a> From<i8> for Value<'a> {
    fn from(val: i8) -> Self {
        Value::Int8(val)
    }
}

impl<'a> From<i32> for Value<'a> {
    fn from(val: i32) -> Self {
        Value::Int32(val)
    }
}

impl<'a> From<f32> for Value<'a> {
    fn from(val: f32) -> Self {
        Value::Float32(val)
    }
}

impl<'a> From<String> for Value<'a> {
    fn from(val: String) -> Self {
        Value::String(Cow::Owned(val.into_bytes()))
    }
}

impl<'a> From<&'a str> for Value<'a> {
    fn from(val: &'a str) -> Self {
        Value::String(Cow::Borrowed(val.as_bytes()))
    }
}

impl<'a> Value<'a> {
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
