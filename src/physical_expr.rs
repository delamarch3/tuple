use crate::evaluate::{concat, contains};
use crate::expr::{
    ArithmeticOperator as ExprArithmeticOperator, ComparisonOperator as ExprComparisonOperator,
    Expr, Function as ExprFunction, Ident as ExprIdent, Literal as ExprLiteral,
    LogicalOperator as ExprLogicalOperator, Operator as ExprOperator,
};
use crate::schema::{Column, Schema};
use crate::tuple::Tuple;
use crate::value::Value;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ArithmeticOperator {
    Add,
    Sub,
    Div,
    Mul,
}

impl ArithmeticOperator {
    pub fn is_commutative(&self) -> bool {
        match self {
            ArithmeticOperator::Add | ArithmeticOperator::Mul => true,
            ArithmeticOperator::Sub | ArithmeticOperator::Div => false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ComparisonOperator {
    Lt,
    Le,
    Eq,
    Neq,
    Ge,
    Gt,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LogicalOperator {
    And,
    Or,
}

pub type Function = for<'a> fn(tuple: &'a Tuple, args: &Vec<PhysicalExpr>) -> Value<'a>;

#[derive(Debug, PartialEq)]
pub enum PhysicalExpr {
    Ident(usize),
    Function(Function, Vec<PhysicalExpr>),
    Value(Value<'static>), // TODO: in base we will need to tie it to the lifetime of the literal
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
    ArithmeticOp {
        lhs: Box<PhysicalExpr>,
        op: ArithmeticOperator,
        rhs: Box<PhysicalExpr>,
    },
    ComparisonOp {
        lhs: Box<PhysicalExpr>,
        op: ComparisonOperator,
        rhs: Box<PhysicalExpr>,
    },
    LogicalOp {
        lhs: Box<PhysicalExpr>,
        op: LogicalOperator,
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
    let position = match ident {
        ExprIdent::Column(name) => schema.find_column(&name).map(Column::position).unwrap(),
        ExprIdent::QualifiedColumn { table, name } => schema
            .find_qualified_column(&table, &name)
            .map(Column::position)
            .unwrap(),
    };

    PhysicalExpr::Ident(position)
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

fn physical_value(literal: ExprLiteral<'static>) -> PhysicalExpr {
    // TODO: coerce if required
    let value = match literal {
        ExprLiteral::Number(number) => Value::Int32(number.parse().unwrap()),
        ExprLiteral::Decimal(decimal) => Value::Float32(decimal.parse().unwrap()),
        ExprLiteral::String(string) => Value::String(string),
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
    let rhs = Box::new(PhysicalExpr::new(rhs, schema));

    match op {
        ExprOperator::Arithmetic(op) => {
            let op = match op {
                ExprArithmeticOperator::Add => ArithmeticOperator::Add,
                ExprArithmeticOperator::Sub => ArithmeticOperator::Sub,
                ExprArithmeticOperator::Div => ArithmeticOperator::Div,
                ExprArithmeticOperator::Mul => ArithmeticOperator::Mul,
            };

            PhysicalExpr::ArithmeticOp { lhs, op, rhs }
        }
        ExprOperator::Comparison(op) => {
            let op = match op {
                ExprComparisonOperator::Lt => ComparisonOperator::Lt,
                ExprComparisonOperator::Le => ComparisonOperator::Le,
                ExprComparisonOperator::Eq => ComparisonOperator::Eq,
                ExprComparisonOperator::Neq => ComparisonOperator::Neq,
                ExprComparisonOperator::Ge => ComparisonOperator::Ge,
                ExprComparisonOperator::Gt => ComparisonOperator::Gt,
            };

            PhysicalExpr::ComparisonOp { lhs, op, rhs }
        }
        ExprOperator::Logical(logical_operator) => {
            let op = match logical_operator {
                ExprLogicalOperator::And => LogicalOperator::And,
                ExprLogicalOperator::Or => LogicalOperator::Or,
            };

            PhysicalExpr::LogicalOp { lhs, op, rhs }
        }
    }
}
