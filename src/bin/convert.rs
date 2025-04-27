use std::env::args;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter};
use std::process::exit;

use tuple::schema::{Schema, Type};
use tuple::tuple::TupleBuilder;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = args();
    let program = args.next().unwrap();

    let (Some(file_path), Some(out_path)) = (args.next(), args.next()) else {
        eprintln!("usage: {program} <in> <out>");
        exit(1);
    };

    let mut lines = File::open(file_path)
        .map(BufReader::new)
        .map(BufReader::lines)
        .map(Iterator::enumerate)?;
    let mut out = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(out_path)
        .map(BufWriter::new)?;

    // partkey     BIGINT not null,
    // name        VARCHAR(55) not null,
    // mfgr        CHAR(25) not null,
    // brand       CHAR(10) not null,
    // type        VARCHAR(25) not null,
    // size        INTEGER not null,
    // container   CHAR(10) not null,
    // retailprice DOUBLE PRECISION not null,
    // comment     VARCHAR(23) not null
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

    while let Some((line_number, result)) = lines.next() {
        let mut tuple = TupleBuilder::new(&schema);
        let line = result?;
        let mut values = line.split('|');
        let mut types = schema.physical_attrs().map(|attr| attr.r#type);
        while let Some(r#type) = types.next() {
            let Some(value) = values.next() else {
                eprintln!("unexpected end of row values on line {line_number}");
                exit(1);
            };

            tuple = match r#type {
                Type::String => tuple.string(value.as_bytes()),
                Type::Int8 => tuple.int8(value.parse()?),
                Type::Int32 => tuple.int32(value.parse()?),
                Type::Float32 => tuple.float32(value.parse()?),
            }
        }

        assert_eq!(values.count(), 1);
        assert_eq!(types.count(), 0);

        tuple.finish().write_to(&mut out)?;
    }

    Ok(())
}
