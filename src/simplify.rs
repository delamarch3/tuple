use crate::physical_expr::{ArithmeticOperator, ComparisonOperator, LogicalOperator, PhysicalExpr};
use crate::value::Value;

type Rule = fn(expr: PhysicalExpr) -> PhysicalExpr;

static RULES: &[Rule] = &[
    simplify_inputs,
    simplify_const,
    simplify_comparison,
    simplify_inequality,
    simplify_logical,
];

pub fn simplify(expr: PhysicalExpr) -> PhysicalExpr {
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

macro_rules! inverse_arithmetic_op {
    ($lhs:expr, $op:expr, $rhs:expr) => {
        match $op {
            ArithmeticOperator::Add => $lhs - $rhs,
            ArithmeticOperator::Sub => $lhs + $rhs,
            ArithmeticOperator::Div => $lhs * $rhs,
            ArithmeticOperator::Mul => $lhs / $rhs,
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

fn simplify_inputs(expr: PhysicalExpr) -> PhysicalExpr {
    match expr {
        PhysicalExpr::ArithmeticOp { lhs, op, rhs } => PhysicalExpr::ArithmeticOp {
            lhs: Box::new(simplify(*lhs)),
            op,
            rhs: Box::new(simplify(*rhs)),
        },
        PhysicalExpr::ComparisonOp { lhs, op, rhs } => PhysicalExpr::ComparisonOp {
            lhs: Box::new(simplify(*lhs)),
            op,
            rhs: Box::new(simplify(*rhs)),
        },
        PhysicalExpr::LogicalOp { lhs, op, rhs } => PhysicalExpr::LogicalOp {
            lhs: Box::new(simplify(*lhs)),
            op,
            rhs: Box::new(simplify(*rhs)),
        },

        e @ (PhysicalExpr::Ident(..)
        | PhysicalExpr::Function(..)
        | PhysicalExpr::Value(..)
        | PhysicalExpr::IsNull { .. }
        | PhysicalExpr::InList { .. }
        | PhysicalExpr::Between { .. }) => e,
    }
}

fn simplify_const(expr: PhysicalExpr) -> PhysicalExpr {
    match expr {
        PhysicalExpr::ArithmeticOp { lhs, op, rhs } => match (*lhs, *rhs) {
            (PhysicalExpr::Value(lhs), PhysicalExpr::Value(rhs)) => {
                PhysicalExpr::Value(arithmetic_op!(lhs, op, rhs))
            }
            (lhs, rhs) => PhysicalExpr::ArithmeticOp {
                lhs: Box::new(lhs),
                op,
                rhs: Box::new(rhs),
            },
        },
        PhysicalExpr::ComparisonOp { lhs, op, rhs } => match (*lhs, *rhs) {
            (PhysicalExpr::Value(lhs), PhysicalExpr::Value(rhs)) => {
                PhysicalExpr::Value(comparison_op!(lhs, op, rhs))
            }
            (lhs, rhs) => PhysicalExpr::ComparisonOp {
                lhs: Box::new(lhs),
                op,
                rhs: Box::new(rhs),
            },
        },
        PhysicalExpr::LogicalOp { lhs, op, rhs } => match (*lhs, *rhs) {
            (PhysicalExpr::Value(lhs), PhysicalExpr::Value(rhs)) => {
                PhysicalExpr::Value(logical_op!(lhs, op, rhs))
            }
            (lhs, rhs) => PhysicalExpr::LogicalOp {
                lhs: Box::new(lhs),
                op,
                rhs: Box::new(rhs),
            },
        },
        _ => expr,
    }
}

fn simplify_inequality(expr: PhysicalExpr) -> PhysicalExpr {
    let PhysicalExpr::ComparisonOp { lhs, op, rhs } = expr else {
        return expr;
    };

    match (*lhs, *rhs) {
        (
            PhysicalExpr::ArithmeticOp {
                lhs: llhs,
                op: aop,
                rhs: lrhs,
            },
            PhysicalExpr::Value(rhs),
        ) => match (*llhs, *lrhs) {
            (llhs, PhysicalExpr::Value(lrhs)) => {
                return PhysicalExpr::ComparisonOp {
                    lhs: Box::new(llhs),
                    op,
                    rhs: Box::new(PhysicalExpr::Value(inverse_arithmetic_op!(rhs, aop, lrhs))),
                };
            }
            (PhysicalExpr::Value(llhs), lrhs) if aop.is_commutative() => {
                return PhysicalExpr::ComparisonOp {
                    lhs: Box::new(lrhs),
                    op,
                    rhs: Box::new(PhysicalExpr::Value(inverse_arithmetic_op!(rhs, aop, llhs))),
                };
            }
            (llhs, lrhs) => PhysicalExpr::ComparisonOp {
                lhs: Box::new(PhysicalExpr::ArithmeticOp {
                    lhs: Box::new(llhs),
                    op: aop,
                    rhs: Box::new(lrhs),
                }),
                op,
                rhs: Box::new(PhysicalExpr::Value(rhs)),
            },
        },
        (
            PhysicalExpr::Value(lhs),
            PhysicalExpr::ArithmeticOp {
                lhs: rlhs,
                op: aop,
                rhs: rrhs,
            },
        ) => match (*rlhs, *rrhs) {
            (rlhs, PhysicalExpr::Value(rrhs)) => {
                return PhysicalExpr::ComparisonOp {
                    lhs: Box::new(PhysicalExpr::Value(inverse_arithmetic_op!(lhs, aop, rrhs))),
                    op,
                    rhs: Box::new(rlhs),
                }
            }
            (PhysicalExpr::Value(rlhs), rrhs) if aop.is_commutative() => {
                return PhysicalExpr::ComparisonOp {
                    lhs: Box::new(PhysicalExpr::Value(inverse_arithmetic_op!(lhs, aop, rlhs))),
                    op,
                    rhs: Box::new(rrhs),
                }
            }
            (rlhs, rrhs) => PhysicalExpr::ComparisonOp {
                lhs: Box::new(PhysicalExpr::Value(lhs)),
                op,
                rhs: Box::new(PhysicalExpr::ArithmeticOp {
                    lhs: Box::new(rlhs),
                    op: aop,
                    rhs: Box::new(rrhs),
                }),
            },
        },
        (lhs, rhs) => PhysicalExpr::ComparisonOp {
            lhs: Box::new(lhs),
            op,
            rhs: Box::new(rhs),
        },
    }
}

fn simplify_comparison(expr: PhysicalExpr) -> PhysicalExpr {
    let PhysicalExpr::ComparisonOp { lhs, op, rhs } = expr else {
        return expr;
    };

    match (*lhs, *rhs) {
        (PhysicalExpr::Ident(c1), PhysicalExpr::Ident(c2)) if c1 == c2 => match op {
            ComparisonOperator::Eq | ComparisonOperator::Le | ComparisonOperator::Ge => {
                PhysicalExpr::Value(Value::Int8(1))
            }
            ComparisonOperator::Lt | ComparisonOperator::Neq | ComparisonOperator::Gt => {
                PhysicalExpr::Value(Value::Int8(0))
            }
        },
        (lhs, rhs) => PhysicalExpr::ComparisonOp {
            lhs: Box::new(lhs),
            op,
            rhs: Box::new(rhs),
        },
    }
}

fn simplify_logical(expr: PhysicalExpr) -> PhysicalExpr {
    let PhysicalExpr::LogicalOp { lhs, op, rhs } = expr else {
        return expr;
    };

    fn simplify_and<'a>(lhs: PhysicalExpr<'a>, rhs: PhysicalExpr<'a>) -> PhysicalExpr<'a> {
        match (lhs, rhs) {
            (expr, PhysicalExpr::Value(rhs)) if !rhs.is_zero() => expr,
            (PhysicalExpr::Value(lhs), expr) if !lhs.is_zero() => expr,
            (PhysicalExpr::Ident(c1), PhysicalExpr::Ident(c2)) if c1 == c2 => {
                PhysicalExpr::Ident(c1)
            }
            (lhs, rhs) => PhysicalExpr::LogicalOp {
                lhs: Box::new(lhs),
                op: LogicalOperator::And,
                rhs: Box::new(rhs),
            },
        }
    }

    fn simplify_or<'a>(lhs: PhysicalExpr<'a>, rhs: PhysicalExpr<'a>) -> PhysicalExpr<'a> {
        match (lhs, rhs) {
            (_, PhysicalExpr::Value(rhs)) if !rhs.is_zero() => PhysicalExpr::Value(Value::Int8(1)),
            (PhysicalExpr::Value(lhs), _) if !lhs.is_zero() => PhysicalExpr::Value(Value::Int8(1)),
            (lhs, rhs) => PhysicalExpr::LogicalOp {
                lhs: Box::new(lhs),
                op: LogicalOperator::Or,
                rhs: Box::new(rhs),
            },
        }
    }

    match op {
        LogicalOperator::And => simplify_and(*lhs, *rhs),
        LogicalOperator::Or => simplify_or(*lhs, *rhs),
    }
}

#[cfg(test)]
mod test {
    use crate::expr::{ident, lit};
    use crate::physical_expr::{
        ArithmeticOperator, ComparisonOperator, LogicalOperator, PhysicalExpr,
    };
    use crate::schema::{Schema, Type};
    use crate::value::Value;

    use super::simplify;

    #[test]
    fn test_simplify() {
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
            (
                ident("c1").add(lit(5)).lt(lit(25)),
                PhysicalExpr::ComparisonOp {
                    lhs: Box::new(PhysicalExpr::Ident(0)),
                    op: ComparisonOperator::Lt,
                    rhs: Box::new(PhysicalExpr::Value(Value::Int32(20))),
                },
            ),
            (
                lit(5).add(ident("c1")).lt(lit(25)),
                PhysicalExpr::ComparisonOp {
                    lhs: Box::new(PhysicalExpr::Ident(0)),
                    op: ComparisonOperator::Lt,
                    rhs: Box::new(PhysicalExpr::Value(Value::Int32(20))),
                },
            ),
            (
                lit(25).gt(ident("c1").add(lit(5))),
                PhysicalExpr::ComparisonOp {
                    lhs: Box::new(PhysicalExpr::Value(Value::Int32(20))),
                    op: ComparisonOperator::Gt,
                    rhs: Box::new(PhysicalExpr::Ident(0)),
                },
            ),
            (
                lit(25).gt(lit(5).add(ident("c1"))),
                PhysicalExpr::ComparisonOp {
                    lhs: Box::new(PhysicalExpr::Value(Value::Int32(20))),
                    op: ComparisonOperator::Gt,
                    rhs: Box::new(PhysicalExpr::Ident(0)),
                },
            ),
            (ident("c1").and(ident("c1")), PhysicalExpr::Ident(0)),
            (
                ident("c1")
                    .and(ident("c1"))
                    .and(ident("c1"))
                    .and(lit(25).gt(ident("c1").add(lit(5)))),
                PhysicalExpr::LogicalOp {
                    lhs: Box::new(PhysicalExpr::Ident(0)),
                    op: LogicalOperator::And,
                    rhs: Box::new(PhysicalExpr::ComparisonOp {
                        lhs: Box::new(PhysicalExpr::Value(Value::Int32(20))),
                        op: ComparisonOperator::Gt,
                        rhs: Box::new(PhysicalExpr::Ident(0)),
                    }),
                },
            ),
            (
                ident("c1")
                    .add(lit(1).sub(lit(1).mul(lit(1).div(lit(1).add(lit(1))))))
                    .and(lit(1).sub(lit(1).mul(lit(1).div(lit(1).add(lit(1)))))),
                PhysicalExpr::ArithmeticOp {
                    lhs: Box::new(PhysicalExpr::Ident(0)),
                    op: ArithmeticOperator::Add,
                    rhs: Box::new(PhysicalExpr::Value(Value::Int32(1))),
                },
            ),
        ]
        .into_iter()
        .for_each(|(expr, want)| {
            let expr = PhysicalExpr::new(expr, &schema);
            let have = simplify(expr);
            assert_eq!(want, have)
        });
    }
}
