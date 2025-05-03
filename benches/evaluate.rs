use std::fs::File;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use tuple::evaluate::evaluate;
use tuple::expr::{concat, contains, ident, lit};
use tuple::physical_expr::PhysicalExpr;
use tuple::schema::{Schema, Type};
use tuple::tuple::Tuple;

fn evaluate_benchmark(c: &mut Criterion) {
    let nullable = false;
    let schema = Schema::default()
        .add_column("partkey".into(), Type::Int32, nullable)
        .add_column("name".into(), Type::String, nullable)
        .add_column("mfgr".into(), Type::String, nullable)
        .add_column("brand".into(), Type::String, nullable)
        .add_column("type".into(), Type::String, nullable)
        .add_column("size".into(), Type::Int32, nullable)
        .add_column("container".into(), Type::String, nullable)
        .add_column("retailprice".into(), Type::Float32, nullable)
        .add_column("comment".into(), Type::String, nullable);

    let mut r = File::open("part.tpls").expect("error opening part.tpls");
    let tuples = std::iter::from_fn(|| match Tuple::read_from(&mut r) {
        Ok(tuple) => Some(Ok(tuple)),
        Err(err) => match err.kind() {
            std::io::ErrorKind::UnexpectedEof => None,
            _ => Some(Err(err)),
        },
    })
    .collect::<Result<Vec<Tuple>, _>>()
    .unwrap();
    assert_eq!(tuples.len(), 200_000);

    let expr = ident("size")
        .add(lit(5))
        .lt(lit(25))
        .and(ident("retailprice").gt(lit(1499.5)));
    let expr = PhysicalExpr::new(expr, &schema);
    c.bench_function("math (size + 5 < 25 and retailprice > 1499.5)", |b| {
        b.iter(|| {
            black_box(tuples.iter().for_each(|tuple| {
                evaluate(&tuple, &expr);
            }))
        });
    });

    let expr = ident("mfgr").eq(lit("Manufacturer#1"));
    let expr = PhysicalExpr::new(expr, &schema);
    c.bench_function("select 1/5 (mfgr == Manufacturer#1)", |b| {
        b.iter(|| {
            black_box(tuples.iter().for_each(|tuple| {
                evaluate(&tuple, &expr);
            }))
        })
    });

    let expr = ident("partkey")
        .add(lit(1).sub(lit(1).mul(lit(1).div(lit(1).add(lit(1))))))
        .and(lit(1).sub(lit(1).mul(lit(1).div(lit(1).add(lit(1))))));
    let expr = PhysicalExpr::new(expr, &schema);
    c.bench_function(
        "long expr (partkey + (1 - 1 * 1 / 1 + 1) and (1 - 1 * 1 / 1 + 1))",
        |b| {
            b.iter(|| {
                black_box(tuples.iter().for_each(|tuple| {
                    evaluate(&tuple, &expr);
                }))
            })
        },
    );

    let expr = ident("partkey").between(lit(1), lit(100_000));
    let expr = PhysicalExpr::new(expr, &schema);
    c.bench_function("between (partkey between 1 and 100_000)", |b| {
        b.iter(|| {
            black_box(tuples.iter().for_each(|tuple| {
                evaluate(&tuple, &expr);
            }))
        })
    });

    let expr = ident("mfgr").in_list(vec![
        lit("Manufacturer#1"),
        lit("Manufacturer#2"),
        lit("Manufacturer#3"),
        lit("Manufacturer#4"),
    ]);
    let expr = PhysicalExpr::new(expr, &schema);
    c.bench_function(
        "in list (mfgr in (Manufacturer#1, Manufacturer#2, Manufacturer#3, Manufacturer#4))",
        |b| {
            b.iter(|| {
                black_box(tuples.iter().for_each(|tuple| {
                    evaluate(&tuple, &expr);
                }))
            })
        },
    );

    let expr = contains(vec![
        concat(vec![ident("container"), ident("container")]),
        lit("CANWRAP"),
    ]);
    let expr = PhysicalExpr::new(expr, &schema);
    c.bench_function(
        "string functions (contains(concat(container, container), CANWRAP)",
        |b| {
            b.iter(|| {
                black_box(tuples.iter().for_each(|tuple| {
                    evaluate(&tuple, &expr);
                }))
            })
        },
    );
}

criterion_group!(benches, evaluate_benchmark);
criterion_main!(benches);
