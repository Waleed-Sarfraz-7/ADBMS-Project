using System.Text.Json.Serialization;
using ConsoleApp1;


class Table
{
    public string Name { get; set; }
    public List<Column> Columns { get; set; }
    public List<Dictionary<string, string>> Rows { get; set; }

    // 👇 NEW: Reference to parent Database (used for FK validation)
    [JsonIgnore]
    public Database ParentDatabase { get; set; }

    public Table(string name, List<Column> columns, Database parentDatabase = null)
    {
        Name = name;
        Columns = columns;
        Rows = new List<Dictionary<string, string>>();
        ParentDatabase = parentDatabase;
    }
    public Table(){}


    public void SetParentDatabase(Database db)
    {
        ParentDatabase = db;
    }


    public void InsertRow(Dictionary<string, string> row)
    {
        if (ParentDatabase == null)
            throw new Exception("Parent database reference is required for constraint validation.");

        // Prepare a finalized row dictionary
        Dictionary<string, string> finalRow = new();

        foreach (var col in Columns)
        {
            string colName = col.Name;

            // 1. Missing value check
            if (!row.ContainsKey(colName))
            {
                if (col.Constraint.Has(ConstraintType.NotNull) || col.Constraint.Has(ConstraintType.PrimaryKey))
                    throw new Exception($"Missing value for NOT NULL column: {colName}");

                finalRow[colName] = null;
                continue;
            }

            string value = row[colName];

            // 2. Data Type Check
            switch (col.Data_Type)
            {
                case "INT":
                    if (!int.TryParse(value, out _))
                        throw new Exception($"Value '{value}' is not a valid INT for column '{colName}'");
                    break;

                case "BOOLEAN":
                    if (!bool.TryParse(value, out _))
                        throw new Exception($"Value '{value}' is not a valid BOOLEAN for column '{colName}'");
                    break;

                case "STRING":
                    if (col.MaxLength.HasValue && value.Length > col.MaxLength.Value)
                        throw new Exception($"Value for '{colName}' exceeds max length of {col.MaxLength.Value}");
                    break;

                default:
                    throw new Exception($"Unknown type: {col.Data_Type} for column '{colName}'");
            }

            // 3. NOT NULL
            if (col.Constraint.Has(ConstraintType.NotNull) && string.IsNullOrEmpty(value))
                throw new Exception($"Column '{colName}' cannot be NULL");

            // 4. UNIQUE (or PRIMARY KEY)
            if (col.Constraint.Has(ConstraintType.Unique) || col.Constraint.Has(ConstraintType.PrimaryKey))
            {
                foreach (var existingRow in Rows)
                {
                    if (existingRow.ContainsKey(colName) && existingRow[colName] == value)
                        throw new Exception($"Value '{value}' for column '{colName}' violates UNIQUE constraint.");
                }
            }

            // 5. CHECK constraint
            //if (!string.IsNullOrEmpty(col.Constraint.CheckExpression))
            //{
            //    string expression = col.Constraint.CheckExpression.Replace("VALUE", value);
            //    try
            //    {
            //        var result = ExpressionEvaluator.Evaluate(expression);
            //        if (!result)
            //            throw new Exception($"Value '{value}' for column '{colName}' violates CHECK constraint: {col.Constraint.CheckExpression}");
            //    }
            //    catch
            //    {
            //        throw new Exception($"Invalid CHECK expression or evaluation failed for column '{colName}'");
            //    }
            //}

            // 6. FOREIGN KEY
            if (col.Constraint.Has(ConstraintType.ForeignKey))
            {
                string refTableName = col.Constraint.ReferenceTable;
                string refColumn = col.Constraint.ReferenceColumn;

                if (!ParentDatabase.Tables.ContainsKey(refTableName))
                    throw new Exception($"Foreign key table '{refTableName}' does not exist.");

                var refTable = ParentDatabase.Tables[refTableName];

                bool found = refTable.Rows.Any(r => r.ContainsKey(refColumn) && r[refColumn] == value);

                if (!found)
                    throw new Exception($"Foreign key constraint failed: '{value}' not found in {refTableName}({refColumn})");
            }

            // 7. Finalize value
            finalRow[colName] = value;
        }

        // All validations passed — add the row
        Rows.Add(finalRow);
    }
    public void UpdateRow(string primaryKeyValue, Dictionary<string, string> updatedValues)
    {
        if (ParentDatabase == null)
            throw new Exception("Parent database reference is required for constraint validation.");

        // Find the primary key column
        var primaryKeyColumn = Columns.FirstOrDefault(c => c.Constraint.Has(ConstraintType.PrimaryKey));
        if (primaryKeyColumn == null)
            throw new Exception("No primary key defined for the table.");

        var targetRow = Rows.FirstOrDefault(r => r[primaryKeyColumn.Name] == primaryKeyValue);
        if (targetRow == null)
            throw new Exception($"No row found with {primaryKeyColumn.Name} = {primaryKeyValue}");

        // Validate and apply each update
        foreach (var update in updatedValues)
        {
            var col = Columns.FirstOrDefault(c => c.Name == update.Key);
            if (col == null)
                throw new Exception($"Column '{update.Key}' does not exist in table '{Name}'.");

            string value = update.Value;

            // Type check
            switch (col.Data_Type)
            {
                case "INT":
                    if (!int.TryParse(value, out _))
                        throw new Exception($"'{value}' is not a valid INT for column '{col.Name}'");
                    break;
                case "BOOLEAN":
                    if (!bool.TryParse(value, out _))
                        throw new Exception($"'{value}' is not a valid BOOLEAN for column '{col.Name}'");
                    break;
                case "STRING":
                    if (col.MaxLength.HasValue && value.Length > col.MaxLength.Value)
                        throw new Exception($"Value for '{col.Name}' exceeds max length {col.MaxLength.Value}");
                    break;
            }

            // Foreign Key Check
            if (col.Constraint.Has(ConstraintType.ForeignKey))
            {
                var refTable = ParentDatabase.Tables[col.Constraint.ReferenceTable];
                var refColumn = col.Constraint.ReferenceColumn;

                bool exists = refTable.Rows.Any(r => r.ContainsKey(refColumn) && r[refColumn] == value);
                if (!exists)
                    throw new Exception($"Foreign key constraint failed for value '{value}' in column '{col.Name}'");
            }

            // Apply update
            targetRow[col.Name] = value;
        }

        Console.WriteLine("Row updated successfully.");
    }

    public void DeleteRow(string primaryKeyValue)
    {
        if (ParentDatabase == null)
            throw new Exception("Parent database reference is required for constraint validation.");

        var primaryKeyColumn = Columns.FirstOrDefault(c => c.Constraint.Has(ConstraintType.PrimaryKey));
        if (primaryKeyColumn == null)
            throw new Exception("No primary key defined for the table.");

        var rowToDelete = Rows.FirstOrDefault(r => r[primaryKeyColumn.Name] == primaryKeyValue);
        if (rowToDelete == null)
            throw new Exception($"No row found with {primaryKeyColumn.Name} = {primaryKeyValue}");

        // Cascade delete from other tables
        foreach (var table in ParentDatabase.Tables.Values)
        {
            foreach (var col in table.Columns)
            {
                if (col.Constraint.Has(ConstraintType.ForeignKey) &&
                    col.Constraint.ReferenceTable == Name &&
                    col.Constraint.ReferenceColumn == primaryKeyColumn.Name)
                {
                    var rowsToCascadeDelete = table.Rows
                        .Where(r => r.ContainsKey(col.Name) && r[col.Name] == primaryKeyValue)
                        .ToList();

                    foreach (var childRow in rowsToCascadeDelete)
                    {
                        table.Rows.Remove(childRow);
                        Console.WriteLine($"Cascading delete: Row removed from '{table.Name}' where '{col.Name}' = {primaryKeyValue}");
                    }
                }
            }
        }

        // Finally delete the row
        Rows.Remove(rowToDelete);
        Console.WriteLine("Row deleted successfully.");
    }

    public void DisplayTable()
    {
        Console.WriteLine($"Table: {Name}");
        Console.WriteLine(string.Join(" | ", Columns.Select(c => c.Name)));
        foreach (var row in Rows)
        {
            Console.WriteLine(string.Join(" | ", Columns.Select(c => row.ContainsKey(c.Name) ? row[c.Name] : "NULL")));
        }
    }
}

