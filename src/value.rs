use std::cmp::Ordering;

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
