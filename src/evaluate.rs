use crate::expr::{
    ArithmeticOperator, ComparisonOperator, Expr, Function, Ident, LogicalOperator, Operator,
};
use crate::schema::{Column, Schema};
use crate::tuple::Tuple;
use crate::value::Value;

pub fn evaluate<'a>(tuple: &'a Tuple, schema: &Schema, expr: &Expr) -> Value<'a> {
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

fn evaluate_ident<'a>(tuple: &'a Tuple, schema: &Schema, ident: &Ident) -> Value<'a> {
    // TODO: Having to find the position here will be slow. The column positions should be cached in
    // the expression.
    let position = match ident {
        Ident::Column(name) => schema.find_column(name).map(Column::position).unwrap(),
        Ident::QualifiedColumn { table, name } => schema
            .find_qualified_column(table, name)
            .map(Column::position)
            .unwrap(),
    };

    tuple.get(schema, position)
}

fn evaluate_function<'a>(tuple: &'a Tuple, schema: &Schema, function: &Function) -> Value<'a> {
    // TODO: This can be improved too. It would be better to have the function implementation as
    // part of the expression, instead of matching here.
    match function.name.as_str() {
        "concat" => evaluate_concat(tuple, schema, &function.args),
        "contains" => evaluate_contains(tuple, schema, &function.args),
        _ => unimplemented!(),
    }
}

fn evaluate_concat<'a>(tuple: &'a Tuple, schema: &Schema, args: &Vec<Expr>) -> Value<'a> {
    let mut s = Vec::new();

    for expr in args {
        let value = evaluate(tuple, schema, expr);
        match value {
            Value::String(value) => s.extend(value.iter()),
            Value::Int8(value) => s.extend(value.to_string().bytes()),
            Value::Int32(value) => s.extend(value.to_string().bytes()),
            Value::Float32(value) => s.extend(value.to_string().bytes()),
            Value::Null => continue,
        }
    }

    Value::String(s.into())
}

fn evaluate_contains<'a>(tuple: &'a Tuple, schema: &Schema, args: &Vec<Expr>) -> Value<'a> {
    let (string, pattern) = (&args[0], &args[1]);

    let Value::String(string) = evaluate(tuple, schema, string) else {
        unimplemented!()
    };

    let Value::String(pattern) = evaluate(tuple, schema, pattern) else {
        unimplemented!()
    };

    let string = std::str::from_utf8(&string).unwrap();
    let pattern = std::str::from_utf8(&pattern).unwrap();

    Value::Int8(string.contains(pattern) as i8)
}

fn evaluate_is_null<'a>(
    tuple: &'a Tuple,
    schema: &Schema,
    expr: &Expr,
    negated: bool,
) -> Value<'a> {
    let value = evaluate(tuple, schema, expr);
    let is_null = matches!(value, Value::Null);
    Value::Int8((is_null && !negated) as i8)
}

fn evaluate_in_list<'a>(
    tuple: &'a Tuple,
    schema: &Schema,
    expr: &Expr,
    list: &Vec<Expr>,
    negated: bool,
) -> Value<'a> {
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

fn evaluate_between<'a>(
    tuple: &'a Tuple,
    schema: &Schema,
    expr: &Expr,
    low: &Expr,
    high: &Expr,
    negated: bool,
) -> Value<'a> {
    let value = evaluate(tuple, schema, expr);
    let low = evaluate(tuple, schema, low);
    let high = evaluate(tuple, schema, high);
    Value::Int8(((value >= low && value <= high) && !negated) as i8)
}

fn evaluate_binary_op<'a>(
    tuple: &'a Tuple,
    schema: &Schema,
    lhs: &Expr,
    op: Operator,
    rhs: &Expr,
) -> Value<'a> {
    let lhs = evaluate(tuple, schema, lhs);
    let rhs = evaluate(tuple, schema, rhs);

    match op {
        Operator::Arithmetic(op) => evaluate_arithmetic_binary_op(lhs, op, rhs),
        Operator::Comparison(op) => evaluate_comparison_binary_op(lhs, op, rhs),
        Operator::Logical(op) => evaluate_logical_binary_op(lhs, op, rhs),
    }
}

fn evaluate_arithmetic_binary_op<'a>(
    lhs: Value<'a>,
    op: ArithmeticOperator,
    rhs: Value<'a>,
) -> Value<'a> {
    match op {
        ArithmeticOperator::Add => lhs + rhs,
        ArithmeticOperator::Sub => lhs - rhs,
        ArithmeticOperator::Div => lhs / rhs,
        ArithmeticOperator::Mul => lhs * rhs,
    }
}

fn evaluate_comparison_binary_op<'a>(
    lhs: Value<'a>,
    op: ComparisonOperator,
    rhs: Value<'a>,
) -> Value<'a> {
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

fn evaluate_logical_binary_op<'a>(
    lhs: Value<'a>,
    op: LogicalOperator,
    rhs: Value<'a>,
) -> Value<'a> {
    let result = match op {
        LogicalOperator::And => !lhs.is_zero() && !rhs.is_zero(),
        LogicalOperator::Or => !lhs.is_zero() || !rhs.is_zero(),
    };

    Value::Int8(result as i8)
}

#[cfg(test)]
mod test {
    use crate::expr::{concat, contains, ident, lit, null};
    use crate::schema::{Schema, Type};
    use crate::tuple::TupleBuilder;
    use crate::value::Value;

    use super::evaluate;

    #[test]
    fn test_constant_expressions() {
        let schema = Schema::default();
        let tuple = TupleBuilder::new(&schema).finish();

        [
            (lit(1 as i32).add(lit(2 as i32)), Value::Int32(3)),
            (lit(1 as i32).add(null()), Value::Int32(1)),
            (lit("a").add(lit("b")), Value::String(b"ab".into())),
            (
                concat(vec![lit("a"), lit("b")]),
                Value::String(b"ab".into()),
            ),
            (contains(vec![lit("abc"), lit("bc")]), Value::Int8(1)),
            (lit(2 as i32).lt(lit(1 as i32)), Value::Int8(0)),
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
                Value::String(b"aa".into()),
            ),
        ]
        .into_iter()
        .for_each(|(expr, want)| {
            let have = evaluate(&tuple, &schema, &expr);
            assert_eq!(want, have);
        });
    }
}
