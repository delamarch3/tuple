#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Type {
    String,
    Int8,
    Int32,
    Float32,
}

impl Type {
    pub fn size(&self) -> usize {
        match self {
            Type::String => 4,
            Type::Int8 => 1,
            Type::Int32 => 4,
            Type::Float32 => 4,
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct Column {
    table: Option<String>,
    name: String,
    r#type: Type,
    offset: usize,
    position: usize,
    nullable: bool,
}

impl Column {
    pub fn position(&self) -> usize {
        self.position
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }
}

#[derive(Default, Clone, PartialEq, Debug)]
pub struct Schema {
    columns: Vec<Column>,
    size: usize,
}

impl Schema {
    pub fn columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn positions(&self) -> impl Iterator<Item = usize> + use<'_> {
        self.columns.iter().map(|Column { position, .. }| *position)
    }

    pub fn types(&self) -> impl Iterator<Item = Type> + use<'_> {
        self.columns.iter().map(|Column { r#type, .. }| *r#type)
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn nulls_size(&self) -> usize {
        (self.columns.len() / 8) + 1
    }

    pub fn get_type(&self, i: usize) -> Type {
        self.columns[i].r#type
    }

    pub fn get_physical_attrs(&self, pos: usize) -> (Type, usize) {
        (self.columns[pos].r#type, self.columns[pos].offset)
    }

    pub fn string_pointer_offsets(&self) -> impl Iterator<Item = usize> + use<'_> {
        self.columns
            .iter()
            .filter(|Column { r#type, .. }| r#type == &Type::String)
            .map(|Column { offset, .. }| *offset)
    }

    pub fn add_column(self, name: String, r#type: Type, nullable: bool) -> Self {
        self.add_qualified_column(None, name, r#type, nullable)
    }

    pub fn add_qualified_column(
        mut self,
        table: Option<String>,
        name: String,
        r#type: Type,
        nullable: bool,
    ) -> Self {
        let offset = self.size;
        self.size += r#type.size();
        self.columns.push(Column {
            table,
            name,
            r#type,
            offset,
            position: self.columns.len(),
            nullable,
        });

        self
    }

    /// Returns a new schema with the provided identifiers only. If a column could not be found,
    /// [`None`] is returned. The column offsets and the size of the schema will remain the same, ie
    /// they may be non-contiguous after projecting. Call [`Schema::compact()`] to make the columns
    /// contiguous.
    pub fn project<'a>(
        &self,
        identifiers: impl Iterator<Item = (Option<&'a str>, &'a str)>,
    ) -> Option<Schema> {
        let mut columns = Vec::new();
        let size = self.size;

        for (table, name) in identifiers {
            let column = match table {
                Some(table) => self.find_qualified_column(table, name).cloned()?,
                None => self.find_column(name).cloned()?,
            };

            columns.push(column);
        }

        Some(Schema { columns, size })
    }

    pub fn compact(&mut self) {
        (_, self.size) = self.columns.iter_mut().fold(
            (0, 0),
            |(i, size),
             Column {
                 r#type,
                 offset,
                 position,
                 ..
             }| {
                *position = i;
                *offset = size;
                (i + 1, size + r#type.size())
            },
        );
    }

    pub fn join(&mut self, other: &Schema) {
        self.columns.extend(other.columns.iter().cloned());
        self.compact();
    }

    pub fn qualify(&mut self, table: &str) {
        self.columns
            .iter_mut()
            .for_each(|column| column.table = Some(table.to_owned()));
    }

    pub fn find_column(&self, name: &str) -> Option<&Column> {
        self.columns
            .iter()
            .find(|Column { name: n1, .. }| name == n1)
    }

    pub fn find_qualified_column(&self, table: &str, name: &str) -> Option<&Column> {
        self.columns.iter().find(
            |Column {
                 table: t1,
                 name: n1,
                 ..
             }| Some(table) == t1.as_deref() && name == n1,
        )
    }
}

#[cfg(test)]
mod test {
    use super::{Schema, Type};

    #[test]
    fn test_project_and_compact() {
        let nullable = true;

        let schema = Schema::default()
            .add_column("c1".into(), Type::Int8, nullable)
            .add_qualified_column(Some("t1".into()), "c2".into(), Type::String, nullable)
            .add_column("c3".into(), Type::Float32, nullable)
            .add_qualified_column(Some("t1".into()), "c4".into(), Type::Int32, nullable);

        let mut want = schema.clone();
        want.columns.remove(1);
        want.columns.remove(2);
        let mut have = schema
            .project([(None, "c1"), (None, "c3")].into_iter())
            .unwrap();
        assert_eq!(want, have);
        let want = Schema::default()
            .add_column("c1".into(), Type::Int8, nullable)
            .add_column("c3".into(), Type::Float32, nullable);
        have.compact();
        assert_eq!(want, have);

        let mut want = schema.clone();
        want.columns.remove(0);
        want.columns.remove(1);
        let mut have = schema
            .project([(Some("t1"), "c2"), (Some("t1"), "c4")].into_iter())
            .unwrap();
        assert_eq!(want, have);
        let want = Schema::default()
            .add_qualified_column(Some("t1".into()), "c2".into(), Type::String, nullable)
            .add_qualified_column(Some("t1".into()), "c4".into(), Type::Int32, nullable);
        have.compact();
        assert_eq!(want, have);
    }

    #[test]
    fn test_join() {
        let nullable = true;

        let lhs = Schema::default()
            .add_column("c1".into(), Type::Int8, nullable)
            .add_qualified_column(Some("t1".into()), "c2".into(), Type::String, nullable)
            .add_column("c3".into(), Type::Float32, nullable)
            .add_qualified_column(Some("t1".into()), "c4".into(), Type::Int32, nullable);

        let rhs = Schema::default();
        let mut have = lhs.clone();
        have.join(&rhs);
        let want = Schema::default()
            .add_column("c1".into(), Type::Int8, nullable)
            .add_qualified_column(Some("t1".into()), "c2".into(), Type::String, nullable)
            .add_column("c3".into(), Type::Float32, nullable)
            .add_qualified_column(Some("t1".into()), "c4".into(), Type::Int32, nullable);
        assert_eq!(want, have);

        let rhs = Schema::default()
            .add_column("c1".into(), Type::Int8, nullable)
            .add_qualified_column(Some("t1".into()), "c2".into(), Type::String, nullable)
            .add_column("c3".into(), Type::Float32, nullable)
            .add_qualified_column(Some("t1".into()), "c4".into(), Type::Int32, nullable);
        let mut have = lhs.clone();
        have.join(&rhs);
        let want = Schema::default()
            .add_column("c1".into(), Type::Int8, nullable)
            .add_qualified_column(Some("t1".into()), "c2".into(), Type::String, nullable)
            .add_column("c3".into(), Type::Float32, nullable)
            .add_qualified_column(Some("t1".into()), "c4".into(), Type::Int32, nullable)
            .add_column("c1".into(), Type::Int8, nullable)
            .add_qualified_column(Some("t1".into()), "c2".into(), Type::String, nullable)
            .add_column("c3".into(), Type::Float32, nullable)
            .add_qualified_column(Some("t1".into()), "c4".into(), Type::Int32, nullable);
        assert_eq!(want, have);
    }
}
