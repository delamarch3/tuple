#[derive(Clone, Copy)]
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

#[derive(Clone)]
pub struct QualifiedColumn {
    table: Option<String>,
    name: String,
}

#[derive(Clone)]
pub struct Column {
    identifier: QualifiedColumn,
    r#type: Type,
    offset: usize,
    position: usize,
    nullable: bool,
}

#[derive(Default, Clone)]
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

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn get_type(&self, i: usize) -> Option<Type> {
        self.columns.get(i).map(|Column { r#type, .. }| *r#type)
    }

    pub fn get_physical_attrs(&self, pos: usize) -> (Type, usize) {
        (self.columns[pos].r#type, self.columns[pos].offset)
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
        let identifier = QualifiedColumn { table, name };
        let offset = self.size;
        self.size += r#type.size();
        self.columns.push(Column {
            identifier,
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
    // TODO: might need variants that accept Vec<usize> and &[&str]
    pub fn project(&self, identifiers: &[QualifiedColumn]) -> Option<Schema> {
        let mut columns = Vec::with_capacity(identifiers.len());
        let size = self.size;

        for QualifiedColumn {
            table: t0,
            name: n0,
        } in identifiers
        {
            let column = self
                .columns
                .iter()
                .find(
                    |Column {
                         identifier:
                             QualifiedColumn {
                                 table: t1,
                                 name: n1,
                             },
                         ..
                     }| t0 == t1 && n0 == n1,
                )?
                .clone();

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
}
