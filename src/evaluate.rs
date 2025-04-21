use crate::schema::{Column, Schema};
use crate::tuple::Tuple;
use crate::value::Value;

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

pub struct Function {
    name: String,
    args: Vec<Expr>,
}

pub enum Ident {
    Column(String),
    QualifiedColumn { table: String, name: String },
}

pub enum Expr {
    Ident(Ident),
    Function(Function),
    Literal(Value),
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    BinaryOp {
        lhs: Box<Expr>,
        op: Operator,
        rhs: Box<Expr>,
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

pub fn lit(value: impl Into<Value>) -> Expr {
    Expr::Literal(value.into())
}

pub fn null() -> Expr {
    Expr::Literal(Value::Null)
}

pub fn concat(args: Vec<Expr>) -> Expr {
    Expr::Function(Function {
        name: "concat".into(),
        args,
    })
}

impl Expr {
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

    pub fn in_list(self, list: Vec<Expr>) -> Self {
        Expr::InList {
            expr: Box::new(self),
            list,
            negated: false,
        }
    }

    pub fn not_in_list(self, list: Vec<Expr>) -> Self {
        Expr::InList {
            expr: Box::new(self),
            list,
            negated: true,
        }
    }

    pub fn between(self, low: impl Into<Expr>, high: impl Into<Expr>) -> Self {
        Expr::Between {
            expr: Box::new(self),
            negated: false,
            low: Box::new(low.into()),
            high: Box::new(high.into()),
        }
    }

    pub fn not_between(self, low: impl Into<Expr>, high: impl Into<Expr>) -> Self {
        Expr::Between {
            expr: Box::new(self),
            negated: true,
            low: Box::new(low.into()),
            high: Box::new(high.into()),
        }
    }

    pub fn add(self, rhs: impl Into<Expr>) -> Self {
        Expr::BinaryOp {
            lhs: Box::new(self),
            op: Operator::Arithmetic(ArithmeticOperator::Add),
            rhs: Box::new(rhs.into()),
        }
    }

    pub fn sub(self, rhs: impl Into<Expr>) -> Self {
        Expr::BinaryOp {
            lhs: Box::new(self),
            op: Operator::Arithmetic(ArithmeticOperator::Sub),
            rhs: Box::new(rhs.into()),
        }
    }

    pub fn mul(self, rhs: impl Into<Expr>) -> Self {
        Expr::BinaryOp {
            lhs: Box::new(self),
            op: Operator::Arithmetic(ArithmeticOperator::Mul),
            rhs: Box::new(rhs.into()),
        }
    }

    pub fn div(self, rhs: impl Into<Expr>) -> Self {
        Expr::BinaryOp {
            lhs: Box::new(self),
            op: Operator::Arithmetic(ArithmeticOperator::Div),
            rhs: Box::new(rhs.into()),
        }
    }

    pub fn eq(self, rhs: impl Into<Expr>) -> Self {
        Expr::BinaryOp {
            lhs: Box::new(self),
            op: Operator::Comparison(ComparisonOperator::Eq),
            rhs: Box::new(rhs.into()),
        }
    }

    pub fn neq(self, rhs: impl Into<Expr>) -> Self {
        Expr::BinaryOp {
            lhs: Box::new(self),
            op: Operator::Comparison(ComparisonOperator::Neq),
            rhs: Box::new(rhs.into()),
        }
    }

    pub fn lt(self, rhs: impl Into<Expr>) -> Self {
        Expr::BinaryOp {
            lhs: Box::new(self),
            op: Operator::Comparison(ComparisonOperator::Lt),
            rhs: Box::new(rhs.into()),
        }
    }

    pub fn le(self, rhs: impl Into<Expr>) -> Self {
        Expr::BinaryOp {
            lhs: Box::new(self),
            op: Operator::Comparison(ComparisonOperator::Le),
            rhs: Box::new(rhs.into()),
        }
    }

    pub fn gt(self, rhs: impl Into<Expr>) -> Self {
        Expr::BinaryOp {
            lhs: Box::new(self),
            op: Operator::Comparison(ComparisonOperator::Gt),
            rhs: Box::new(rhs.into()),
        }
    }

    pub fn ge(self, rhs: impl Into<Expr>) -> Self {
        Expr::BinaryOp {
            lhs: Box::new(self),
            op: Operator::Comparison(ComparisonOperator::Ge),
            rhs: Box::new(rhs.into()),
        }
    }

    pub fn and(self, rhs: impl Into<Expr>) -> Self {
        Expr::BinaryOp {
            lhs: Box::new(self),
            op: Operator::Logical(LogicalOperator::And),
            rhs: Box::new(rhs.into()),
        }
    }

    pub fn or(self, rhs: impl Into<Expr>) -> Self {
        Expr::BinaryOp {
            lhs: Box::new(self),
            op: Operator::Logical(LogicalOperator::Or),
            rhs: Box::new(rhs.into()),
        }
    }
}

pub fn evaluate(tuple: &Tuple, schema: &Schema, expr: &Expr) -> Value {
    match expr {
        Expr::Ident(ident) => evaluate_ident(tuple, schema, ident),
        Expr::Function(function) => evaluate_function(tuple, schema, function),
        Expr::Literal(value) => value.clone(),
        Expr::IsNull { expr, negated } => evaluate_is_null(tuple, schema, expr, *negated),
        Expr::InList {
            expr,
            list,
            negated,
        } => evaluate_in_list(tuple, schema, expr, list, *negated),
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => evaluate_between(tuple, schema, expr, low, high, *negated),
        Expr::BinaryOp { lhs, op, rhs } => evaluate_binary_op(tuple, schema, lhs, *op, rhs),
    }
}

fn evaluate_ident(tuple: &Tuple, schema: &Schema, ident: &Ident) -> Value {
    // TODO: Having to find the position here will be slow. The column positions should be cached in
    // the expression.
    let position = match ident {
        Ident::Column(name) => schema.find_column(name).map(Column::position).unwrap(),
        Ident::QualifiedColumn { table, name } => schema
            .find_qualified_column(Some(table), name)
            .map(Column::position)
            .unwrap(),
    };

    tuple.get(schema, position)
}

fn evaluate_function(tuple: &Tuple, schema: &Schema, function: &Function) -> Value {
    // TODO: This can be improved too. It would be better to have the function implementation as
    // part of the expression, instead of matching here.
    match function.name.as_str() {
        "concat" => evaluate_concat(tuple, schema, &function.args),
        _ => unimplemented!(),
    }
}

fn evaluate_concat(tuple: &Tuple, schema: &Schema, args: &Vec<Expr>) -> Value {
    let mut s = Vec::new();

    for expr in args {
        let value = evaluate(tuple, schema, expr);
        match value {
            Value::String(value) => s.extend(value),
            Value::Int8(value) => s.extend(value.to_string().bytes()),
            Value::Int32(value) => s.extend(value.to_string().bytes()),
            Value::Float32(value) => s.extend(value.to_string().bytes()),
            Value::Null => continue,
        }
    }

    Value::String(s)
}

fn evaluate_is_null(tuple: &Tuple, schema: &Schema, expr: &Expr, negated: bool) -> Value {
    let value = evaluate(tuple, schema, expr);
    let is_null = matches!(value, Value::Null);
    Value::Int8((is_null && !negated) as i8)
}

fn evaluate_in_list(
    tuple: &Tuple,
    schema: &Schema,
    expr: &Expr,
    list: &Vec<Expr>,
    negated: bool,
) -> Value {
    let mut found = false;
    let search = evaluate(tuple, schema, expr);
    for expr in list {
        let value = evaluate(tuple, schema, expr);
        if search == value {
            found = true;
            break;
        }
    }

    Value::Int8((found && !negated) as i8)
}

fn evaluate_between(
    tuple: &Tuple,
    schema: &Schema,
    expr: &Expr,
    low: &Expr,
    high: &Expr,
    negated: bool,
) -> Value {
    let value = evaluate(tuple, schema, expr);
    let low = evaluate(tuple, schema, low);
    let high = evaluate(tuple, schema, high);
    Value::Int8(((value >= low && value <= high) && !negated) as i8)
}

fn evaluate_binary_op(
    tuple: &Tuple,
    schema: &Schema,
    lhs: &Expr,
    op: Operator,
    rhs: &Expr,
) -> Value {
    let lhs = evaluate(tuple, schema, lhs);
    let rhs = evaluate(tuple, schema, rhs);

    match op {
        Operator::Arithmetic(op) => evaluate_arithmetic_binary_op(lhs, op, rhs),
        Operator::Comparison(op) => evaluate_comparison_binary_op(lhs, op, rhs),
        Operator::Logical(op) => evaluate_logical_binary_op(lhs, op, rhs),
    }
}

fn evaluate_arithmetic_binary_op(lhs: Value, op: ArithmeticOperator, rhs: Value) -> Value {
    match op {
        ArithmeticOperator::Add => lhs + rhs,
        ArithmeticOperator::Sub => lhs - rhs,
        ArithmeticOperator::Div => lhs / rhs,
        ArithmeticOperator::Mul => lhs * rhs,
    }
}

fn evaluate_comparison_binary_op(lhs: Value, op: ComparisonOperator, rhs: Value) -> Value {
    let result = match op {
        ComparisonOperator::Lt => lhs < rhs,
        ComparisonOperator::Le => lhs <= rhs,
        ComparisonOperator::Eq => lhs == rhs,
        ComparisonOperator::Neq => lhs != rhs,
        ComparisonOperator::Ge => lhs >= rhs,
        ComparisonOperator::Gt => lhs > rhs,
    };

    Value::Int8(result as i8)
}

fn evaluate_logical_binary_op(lhs: Value, op: LogicalOperator, rhs: Value) -> Value {
    let result = match op {
        LogicalOperator::And => !lhs.is_zero() && !rhs.is_zero(),
        LogicalOperator::Or => !lhs.is_zero() || !rhs.is_zero(),
    };

    Value::Int8(result as i8)
}

#[cfg(test)]
mod test {
    use crate::schema::{Schema, Type};
    use crate::tuple::TupleBuilder;
    use crate::value::Value;

    use super::{concat, evaluate, ident, lit, null};

    #[test]
    fn test_constant_expressions() {
        let schema = Schema::default();
        let tuple = TupleBuilder::new(&schema).finish();

        [
            (lit(1 as i32).add(lit(2 as i32)), Value::Int32(3)),
            (lit(1 as i32).add(null()), Value::Int32(1)),
            (lit("a").add(lit("b")), Value::String(b"ab".to_vec())),
            (
                concat(vec![lit("a"), lit("b")]),
                Value::String(b"ab".to_vec()),
            ),
        ]
        .into_iter()
        .for_each(|(expr, want)| {
            let have = evaluate(&tuple, &schema, &expr);
            assert_eq!(want, have);
        });
    }

    #[test]
    fn test_expressions() {
        let nullable = true;
        let schema = Schema::default()
            .add_column("c1".into(), Type::Int8, nullable)
            .add_qualified_column(Some("t1".into()), "c2".into(), Type::String, nullable)
            .add_column("c3".into(), Type::Float32, nullable)
            .add_qualified_column(Some("t1".into()), "c4".into(), Type::Int32, nullable);
        let tuple = TupleBuilder::new(&schema)
            .null()
            .string(b"a")
            .float32(7.2)
            .int32(32)
            .finish();

        [
            (ident("c1").eq(ident("t1.c2")), Value::Int8(0)),
            (ident("t1.c4").div(lit(4)), Value::Int32(8)),
            (ident("c1").is_null(), Value::Int8(1)),
            (ident("c3").between(lit(1.), lit(10.)), Value::Int8(1)),
            (
                ident("c3").in_list(vec![lit(1.), lit(10.), lit(7.2)]),
                Value::Int8(1),
            ),
            (
                ident("t1.c2").add(ident("t1.c2")),
                Value::String(b"aa".to_vec()),
            ),
        ]
        .into_iter()
        .for_each(|(expr, want)| {
            let have = evaluate(&tuple, &schema, &expr);
            assert_eq!(want, have);
        });
    }
}
