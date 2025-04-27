use std::borrow::Cow;

use crate::evaluate::{concat, contains};
use crate::expr::{
    ArithmeticOperator as ExprArithmeticOperator, ComparisonOperator as ExprComparisonOperator,
    Expr, Function as ExprFunction, Ident as ExprIdent, Literal as ExprLiteral,
    LogicalOperator as ExprLogicalOperator, Operator as ExprOperator,
};
use crate::schema::{Column, Schema, Type};
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

pub type Function = for<'a> fn(tuple: &'a Tuple, args: &Vec<PhysicalExpr>) -> Value<'a>;

pub struct ValueAttrs {
    pub r#type: Type,
    pub position: usize,
    pub offset: usize,
}

pub enum PhysicalExpr {
    Ident(ValueAttrs),
    Function(Function, Vec<PhysicalExpr>),
    Value(Value<'static>),
    IsNull {
        expr: Box<PhysicalExpr>,
        negated: bool,
    },
    InList {
        expr: Box<PhysicalExpr>,
        list: Vec<PhysicalExpr>,
        negated: bool,
    },
    Between {
        expr: Box<PhysicalExpr>,
        low: Box<PhysicalExpr>,
        high: Box<PhysicalExpr>,
        negated: bool,
    },
    BinaryOp {
        lhs: Box<PhysicalExpr>,
        op: Operator,
        rhs: Box<PhysicalExpr>,
    },
}

impl PhysicalExpr {
    pub fn new(expr: Expr, schema: &Schema) -> Self {
        match expr {
            Expr::Ident(ident) => physical_ident(ident, schema),
            Expr::Function(function) => physical_function(function, schema),
            Expr::Literal(literal) => physical_value(literal),
            Expr::IsNull { expr, negated } => physical_is_null(*expr, negated, schema),
            Expr::InList {
                expr,
                list,
                negated,
            } => physical_in_list(*expr, list, negated, schema),
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => physical_between(*expr, *high, *low, negated, schema),
            Expr::BinaryOp { lhs, op, rhs } => physical_binary_op(*lhs, op, *rhs, schema),
        }
    }
}

fn physical_ident(ident: ExprIdent, schema: &Schema) -> PhysicalExpr {
    let (r#type, position, offset) = match ident {
        ExprIdent::Column(name) => schema
            .find_column(&name)
            .map(Column::physical_attrs)
            .unwrap(),
        ExprIdent::QualifiedColumn { table, name } => schema
            .find_qualified_column(&table, &name)
            .map(Column::physical_attrs)
            .unwrap(),
    };

    PhysicalExpr::Ident(ValueAttrs {
        r#type,
        position,
        offset,
    })
}

fn physical_function(function: ExprFunction, schema: &Schema) -> PhysicalExpr {
    let args = function
        .args
        .into_iter()
        .map(|expr| PhysicalExpr::new(expr, schema))
        .collect();
    match function.name.as_str() {
        "concat" => PhysicalExpr::Function(concat, args),
        "contains" => PhysicalExpr::Function(contains, args),
        _ => unimplemented!(),
    }
}

fn physical_value(literal: ExprLiteral) -> PhysicalExpr {
    // TODO: coerce if required
    let value = match literal {
        ExprLiteral::Number(number) => Value::Int32(number.parse().unwrap()),
        ExprLiteral::Decimal(decimal) => Value::Float32(decimal.parse().unwrap()),
        ExprLiteral::String(string) => Value::String(Cow::Owned(string.into_bytes())),
        ExprLiteral::Bool(bool) => Value::Int8(bool as i8),
        ExprLiteral::Null => Value::Null,
    };

    PhysicalExpr::Value(value)
}

fn physical_is_null(expr: Expr, negated: bool, schema: &Schema) -> PhysicalExpr {
    let expr = Box::new(PhysicalExpr::new(expr, schema));
    PhysicalExpr::IsNull { expr, negated }
}

fn physical_in_list(expr: Expr, list: Vec<Expr>, negated: bool, schema: &Schema) -> PhysicalExpr {
    let expr = Box::new(PhysicalExpr::new(expr, schema));
    let list = list
        .into_iter()
        .map(|expr| PhysicalExpr::new(expr, schema))
        .collect();
    PhysicalExpr::InList {
        expr,
        list,
        negated,
    }
}

fn physical_between(
    expr: Expr,
    high: Expr,
    low: Expr,
    negated: bool,
    schema: &Schema,
) -> PhysicalExpr {
    let expr = Box::new(PhysicalExpr::new(expr, schema));
    let high = Box::new(PhysicalExpr::new(high, schema));
    let low = Box::new(PhysicalExpr::new(low, schema));
    PhysicalExpr::Between {
        expr,
        low,
        high,
        negated,
    }
}

fn physical_binary_op(lhs: Expr, op: ExprOperator, rhs: Expr, schema: &Schema) -> PhysicalExpr {
    let lhs = Box::new(PhysicalExpr::new(lhs, schema));
    let op = physical_operator(op);
    let rhs = Box::new(PhysicalExpr::new(rhs, schema));
    PhysicalExpr::BinaryOp { lhs, op, rhs }
}

fn physical_operator(op: ExprOperator) -> Operator {
    match op {
        ExprOperator::Arithmetic(arithmetic_operator) => {
            Operator::Arithmetic(match arithmetic_operator {
                ExprArithmeticOperator::Add => ArithmeticOperator::Add,
                ExprArithmeticOperator::Sub => ArithmeticOperator::Sub,
                ExprArithmeticOperator::Div => ArithmeticOperator::Div,
                ExprArithmeticOperator::Mul => ArithmeticOperator::Mul,
            })
        }
        ExprOperator::Comparison(comparison_operator) => {
            Operator::Comparison(match comparison_operator {
                ExprComparisonOperator::Lt => ComparisonOperator::Lt,
                ExprComparisonOperator::Le => ComparisonOperator::Le,
                ExprComparisonOperator::Eq => ComparisonOperator::Eq,
                ExprComparisonOperator::Neq => ComparisonOperator::Neq,
                ExprComparisonOperator::Ge => ComparisonOperator::Ge,
                ExprComparisonOperator::Gt => ComparisonOperator::Gt,
            })
        }
        ExprOperator::Logical(logical_operator) => Operator::Logical(match logical_operator {
            ExprLogicalOperator::And => LogicalOperator::And,
            ExprLogicalOperator::Or => LogicalOperator::Or,
        }),
    }
}
