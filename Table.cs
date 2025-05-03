using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;

namespace ConsoleApp1
{
    [DataContract]
    class Table
    {
        [DataMember]
        public string Name { get; set; }
        [DataMember]
        public List<Column> Columns { get; set; }
        [DataMember]
        public List<Dictionary<string, string>> Rows { get; set; }

        // Store the Index objects instead of directly storing BTree
        [DataMember]
        public Dictionary<string, Index> Indexes { get; set; } = new();

        // Reference to parent Database (used for FK validation)
        [IgnoreDataMember]
        private Database _parentDatabase;

        public Database ParentDatabase
        {
            get => _parentDatabase;
            set => _parentDatabase = value;
        }

        public Table(string name, List<Column> columns, Database parentDatabase = null)
        {
            Name = name;
            Columns = columns;
            Rows = new List<Dictionary<string, string>>();
            ParentDatabase = parentDatabase;
        }

        public Table() { }

        // Create an index for a column using the Index class (which internally uses BTree)
        public void CreateIndex(string columnName)
        {
            var column = Columns.FirstOrDefault(c => c.Name == columnName);
            if (column == null)
                throw new Exception($"Column '{columnName}' not found in table '{Name}'.");

            // Create an Index object for the column and store it in the Indexes dictionary
            var index = new Index(columnName);

            // Populate the index with the existing rows
            foreach (var row in Rows)
            {
                var value = row[columnName];
                if (value != null)
                {
                    // Add the value to the index (BTree inside Index)
                    index.AddToIndex(value, row);
                }
            }

            // Add the index to the dictionary
            Indexes[columnName] = index;
        }

        public bool HasColumn(string columnName)
        {
            return Columns.Any(column => column.Name == columnName);
        }

        public void RestoreIndexes()
        {
            // Restore all indexes from their serialized form
            foreach (var index in Indexes.Values)
            {
                index.RestoreAfterLoad();
            }
        }

        // Lookup a row using the index (BTree) for a specific column and key
        public object LookupUsingIndex(string columnName, int key)
        {
            if (Indexes.TryGetValue(columnName, out var index))
            {
                return index.Lookup(key);
            }
            return null;
        }

        public object LookupUsingIndex(string columnName, string key)
        {
            if (Indexes.TryGetValue(columnName, out var index))
            {
                return index.Lookup(key);
            }
            return null;
        }

        public void SetParentDatabase(Database db)
        {
            ParentDatabase = db;
        }

        // Insert a new row into the table and update the indexes
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

            // After insertion, update relevant indexes
            foreach (var col in Columns)
            {
                if (Indexes.ContainsKey(col.Name))
                {
                    Index index = new Index(col.Name);
                    index.AddToIndex(finalRow[col.Name], finalRow);
                }
            }
        }

        // Update a row in the table and update indexes as necessary
        public void UpdateRow(string primaryKeyValue, Dictionary<string, string> updatedValues)
        {
            if (ParentDatabase == null)
                throw new Exception("Parent database reference is required for constraint validation.");

            var primaryKeyColumn = Columns.FirstOrDefault(c => c.Constraint.Has(ConstraintType.PrimaryKey));
            if (primaryKeyColumn == null)
                throw new Exception("No primary key defined for the table.");

            var targetRow = Rows.FirstOrDefault(r => r[primaryKeyColumn.Name] == primaryKeyValue);
            if (targetRow == null)
                throw new Exception($"No row found with {primaryKeyColumn.Name} = {primaryKeyValue}");

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

                // After updating, update relevant indexes
                if (Indexes.ContainsKey(col.Name))
                {
                    var index = Indexes[col.Name];
                    index.AddToIndex(value, targetRow);
                }
            }

            Console.WriteLine("Row updated successfully.");
        }

        // Delete a row from the table and cascade delete as necessary
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

            // Finally delete the row and update indexes
            Rows.Remove(rowToDelete);

            foreach (var index in Indexes.Values)
            {
                index.AddToIndex(primaryKeyValue, null); // remove from the index as well
            }

            Console.WriteLine("Row deleted successfully.");
        }

        // Display table data
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
}
