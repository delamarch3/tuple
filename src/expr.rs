use std::borrow::Cow;

#[derive(Clone, Copy)]
pub enum ArithmeticOperator {
    Add,
    Sub,
    Div,
    Mul,
}

#[derive(Clone, Copy)]
pub enum ComparisonOperator {
    Lt,
    Le,
    Eq,
    Neq,
    Ge,
    Gt,
}

#[derive(Clone, Copy)]
pub enum LogicalOperator {
    And,
    Or,
}

#[derive(Clone, Copy)]
pub enum Operator {
    Arithmetic(ArithmeticOperator),
    Comparison(ComparisonOperator),
    Logical(LogicalOperator),
}

pub struct Function<'a> {
    pub name: String,
    pub args: Vec<Expr<'a>>,
}

pub enum Ident {
    Column(String),
    QualifiedColumn { table: String, name: String },
}

pub enum Literal<'a> {
    Number(String),
    Decimal(String),
    String(Cow<'a, [u8]>),
    Bool(bool),
    Null,
}

impl<'a> From<i8> for Literal<'a> {
    fn from(val: i8) -> Self {
        Literal::Number(val.to_string())
    }
}

impl<'a> From<i32> for Literal<'a> {
    fn from(val: i32) -> Self {
        Literal::Number(val.to_string())
    }
}

impl<'a> From<f32> for Literal<'a> {
    fn from(val: f32) -> Self {
        Literal::Decimal(val.to_string())
    }
}

impl<'a> From<&'a str> for Literal<'a> {
    fn from(val: &'a str) -> Self {
        Literal::String(Cow::Borrowed(val.as_bytes()))
    }
}

impl<'a> From<String> for Literal<'a> {
    fn from(val: String) -> Self {
        Literal::String(Cow::Owned(val.into_bytes()))
    }
}

pub enum Expr<'a> {
    Ident(Ident),
    Function(Function<'a>),
    Literal(Literal<'a>),
    IsNull {
        expr: Box<Expr<'a>>,
        negated: bool,
    },
    InList {
        expr: Box<Expr<'a>>,
        list: Vec<Expr<'a>>,
        negated: bool,
    },
    Between {
        expr: Box<Expr<'a>>,
        low: Box<Expr<'a>>,
        high: Box<Expr<'a>>,
        negated: bool,
    },
    BinaryOp {
        lhs: Box<Expr<'a>>,
        op: Operator,
        rhs: Box<Expr<'a>>,
    },
}

pub fn ident(value: &str) -> Expr {
    let mut idents = value.split('.').rev();
    let name = idents.next().map(ToString::to_string).unwrap();
    let table = idents.next().map(ToString::to_string);
    if idents.count() > 0 {
        panic!("unsupported identifier: {}", value)
    }

    match table {
        Some(table) => Expr::Ident(Ident::QualifiedColumn { table, name }),
        None => Expr::Ident(Ident::Column(name)),
    }
}

pub fn lit<'a>(value: impl Into<Literal<'a>>) -> Expr<'a> {
    Expr::Literal(value.into())
}

pub fn null() -> Expr<'static> {
    Expr::Literal(Literal::Null)
}

pub fn concat(args: Vec<Expr>) -> Expr {
    Expr::Function(Function {
        name: "concat".into(),
        args,
    })
}

pub fn contains(args: Vec<Expr>) -> Expr {
    Expr::Function(Function {
        name: "contains".into(),
        args,
    })
}

impl<'a> Expr<'a> {
    pub fn is_null(self) -> Self {
        Expr::IsNull {
            expr: Box::new(self),
            negated: false,
        }
    }

    pub fn is_not_null(self) -> Self {
        Expr::IsNull {
            expr: Box::new(self),
            negated: true,
        }
    }

    pub fn in_list(self, list: Vec<Expr<'a>>) -> Self {
        Expr::InList {
            expr: Box::new(self),
            list,
            negated: false,
        }
    }

    pub fn not_in_list(self, list: Vec<Expr<'a>>) -> Self {
        Expr::InList {
            expr: Box::new(self),
            list,
            negated: true,
        }
    }

    pub fn between(self, low: impl Into<Expr<'a>>, high: impl Into<Expr<'a>>) -> Self {
        Expr::Between {
            expr: Box::new(self),
            negated: false,
            low: Box::new(low.into()),
            high: Box::new(high.into()),
        }
    }

    pub fn not_between(self, low: impl Into<Expr<'a>>, high: impl Into<Expr<'a>>) -> Self {
        Expr::Between {
            expr: Box::new(self),
            negated: true,
            low: Box::new(low.into()),
            high: Box::new(high.into()),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn add(self, rhs: impl Into<Expr<'a>>) -> Self {
        Expr::BinaryOp {
            lhs: Box::new(self),
            op: Operator::Arithmetic(ArithmeticOperator::Add),
            rhs: Box::new(rhs.into()),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn sub(self, rhs: impl Into<Expr<'a>>) -> Self {
        Expr::BinaryOp {
            lhs: Box::new(self),
            op: Operator::Arithmetic(ArithmeticOperator::Sub),
            rhs: Box::new(rhs.into()),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn mul(self, rhs: impl Into<Expr<'a>>) -> Self {
        Expr::BinaryOp {
            lhs: Box::new(self),
            op: Operator::Arithmetic(ArithmeticOperator::Mul),
            rhs: Box::new(rhs.into()),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn div(self, rhs: impl Into<Expr<'a>>) -> Self {
        Expr::BinaryOp {
            lhs: Box::new(self),
            op: Operator::Arithmetic(ArithmeticOperator::Div),
            rhs: Box::new(rhs.into()),
        }
    }

    pub fn eq(self, rhs: impl Into<Expr<'a>>) -> Self {
        Expr::BinaryOp {
            lhs: Box::new(self),
            op: Operator::Comparison(ComparisonOperator::Eq),
            rhs: Box::new(rhs.into()),
        }
    }

    pub fn neq(self, rhs: impl Into<Expr<'a>>) -> Self {
        Expr::BinaryOp {
            lhs: Box::new(self),
            op: Operator::Comparison(ComparisonOperator::Neq),
            rhs: Box::new(rhs.into()),
        }
    }

    pub fn lt(self, rhs: impl Into<Expr<'a>>) -> Self {
        Expr::BinaryOp {
            lhs: Box::new(self),
            op: Operator::Comparison(ComparisonOperator::Lt),
            rhs: Box::new(rhs.into()),
        }
    }

    pub fn le(self, rhs: impl Into<Expr<'a>>) -> Self {
        Expr::BinaryOp {
            lhs: Box::new(self),
            op: Operator::Comparison(ComparisonOperator::Le),
            rhs: Box::new(rhs.into()),
        }
    }

    pub fn gt(self, rhs: impl Into<Expr<'a>>) -> Self {
        Expr::BinaryOp {
            lhs: Box::new(self),
            op: Operator::Comparison(ComparisonOperator::Gt),
            rhs: Box::new(rhs.into()),
        }
    }

    pub fn ge(self, rhs: impl Into<Expr<'a>>) -> Self {
        Expr::BinaryOp {
            lhs: Box::new(self),
            op: Operator::Comparison(ComparisonOperator::Ge),
            rhs: Box::new(rhs.into()),
        }
    }

    pub fn and(self, rhs: impl Into<Expr<'a>>) -> Self {
        Expr::BinaryOp {
            lhs: Box::new(self),
            op: Operator::Logical(LogicalOperator::And),
            rhs: Box::new(rhs.into()),
        }
    }

    pub fn or(self, rhs: impl Into<Expr<'a>>) -> Self {
        Expr::BinaryOp {
            lhs: Box::new(self),
            op: Operator::Logical(LogicalOperator::Or),
            rhs: Box::new(rhs.into()),
        }
    }
}
