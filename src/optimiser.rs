use crate::physical_expr::{ArithmeticOperator, ComparisonOperator, LogicalOperator, PhysicalExpr};
use crate::value::Value;

type Rule = fn(expr: PhysicalExpr) -> PhysicalExpr;

static RULES: &[Rule] = &[optimise_binary_op];

pub fn optimise(expr: PhysicalExpr) -> PhysicalExpr {
    RULES.iter().fold(expr, |expr, rule| rule(expr))
}

macro_rules! arithmetic_op {
    ($lhs:expr, $op:expr, $rhs:expr) => {
        match $op {
            ArithmeticOperator::Add => $lhs + $rhs,
            ArithmeticOperator::Sub => $lhs - $rhs,
            ArithmeticOperator::Div => $lhs / $rhs,
            ArithmeticOperator::Mul => $lhs * $rhs,
        }
    };
}

macro_rules! comparison_op {
    ($lhs:expr, $op:expr, $rhs:expr) => {
        Value::Int8(match $op {
            ComparisonOperator::Lt => $lhs < $rhs,
            ComparisonOperator::Le => $lhs <= $rhs,
            ComparisonOperator::Eq => $lhs == $rhs,
            ComparisonOperator::Neq => $lhs != $rhs,
            ComparisonOperator::Ge => $lhs >= $rhs,
            ComparisonOperator::Gt => $lhs > $rhs,
        } as i8)
    };
}

macro_rules! logical_op {
    ($lhs:expr, $op:expr, $rhs:expr) => {
        Value::Int8(match $op {
            LogicalOperator::And => !$lhs.is_zero() && !$rhs.is_zero(),
            LogicalOperator::Or => !$lhs.is_zero() || !$rhs.is_zero(),
        } as i8)
    };
}

fn optimise_binary_op(expr: PhysicalExpr) -> PhysicalExpr {
    fn optimise_binary_op_inner<O, M, N>(
        lhs: PhysicalExpr,
        op: O,
        rhs: PhysicalExpr,
        matched: M,
        nomatch: N,
    ) -> PhysicalExpr
    where
        M: for<'a> Fn(Value<'a>, Value<'a>) -> Value<'a>,
        N: Fn(Box<PhysicalExpr>, O, Box<PhysicalExpr>) -> PhysicalExpr,
    {
        let lhs = optimise(lhs);
        let rhs = optimise(rhs);

        match (lhs, rhs) {
            (PhysicalExpr::Value(lhs), PhysicalExpr::Value(rhs)) => {
                PhysicalExpr::Value(matched(lhs, rhs))
            }
            (lhs, rhs) => nomatch(Box::new(lhs), op, Box::new(rhs)),
        }
    }

    match expr {
        PhysicalExpr::ArithmeticOp { lhs, op, rhs } => optimise_binary_op_inner(
            *lhs,
            op,
            *rhs,
            |lhs, rhs| arithmetic_op!(lhs, op, rhs),
            |lhs, op, rhs| PhysicalExpr::ArithmeticOp { lhs, op, rhs },
        ),
        PhysicalExpr::ComparisonOp { lhs, op, rhs } => optimise_binary_op_inner(
            *lhs,
            op,
            *rhs,
            |lhs, rhs| comparison_op!(lhs, op, rhs),
            |lhs, op, rhs| PhysicalExpr::ComparisonOp { lhs, op, rhs },
        ),
        PhysicalExpr::LogicalOp { lhs, op, rhs } => optimise_binary_op_inner(
            *lhs,
            op,
            *rhs,
            |lhs, rhs| logical_op!(lhs, op, rhs),
            |lhs, op, rhs| PhysicalExpr::LogicalOp { lhs, op, rhs },
        ),
        _ => expr,
    }
}

#[cfg(test)]
mod test {
    use crate::expr::{ident, lit};
    use crate::physical_expr::{ArithmeticOperator, PhysicalExpr};
    use crate::schema::{Schema, Type};
    use crate::value::Value;

    use super::optimise;

    #[test]
    fn test_optimise() {
        let nullable = false;
        let schema = Schema::default()
            .add_column("c1".into(), Type::Int32, nullable)
            .add_column("c2".into(), Type::Int32, nullable);

        [
            (
                lit(1 as i32).add(lit(1 as i32)),
                PhysicalExpr::Value(Value::Int32(2)),
            ),
            (
                lit(1 as i32).add(lit(1 as i32).add(lit(1 as i32))),
                PhysicalExpr::Value(Value::Int32(3)),
            ),
            (
                ident("c1").add(lit(1 as i32).add(lit(1 as i32))),
                PhysicalExpr::ArithmeticOp {
                    lhs: Box::new(PhysicalExpr::Ident(0)),
                    op: ArithmeticOperator::Add,
                    rhs: Box::new(PhysicalExpr::Value(Value::Int32(2))),
                },
            ),
            (
                lit(1 as i32).add(lit(1 as i32)).add(ident("c1")),
                PhysicalExpr::ArithmeticOp {
                    lhs: Box::new(PhysicalExpr::Value(Value::Int32(2))),
                    op: ArithmeticOperator::Add,
                    rhs: Box::new(PhysicalExpr::Ident(0)),
                },
            ),
            (
                ident("c1").add(ident("c2")),
                PhysicalExpr::ArithmeticOp {
                    lhs: Box::new(PhysicalExpr::Ident(0)),
                    op: ArithmeticOperator::Add,
                    rhs: Box::new(PhysicalExpr::Ident(1)),
                },
            ),
        ]
        .into_iter()
        .for_each(|(expr, want)| {
            let expr = PhysicalExpr::new(expr, &schema);
            let have = optimise(expr);
            assert_eq!(want, have)
        });
    }
}
