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
        public void UpdateIndexesForUpdate(Dictionary<string, string> oldRow, Dictionary<string, string> newRow)
        {
            foreach (var kvp in Indexes)
            {
                var column = kvp.Key;
                var index = kvp.Value;

                oldRow.TryGetValue(column, out var oldVal);
                newRow.TryGetValue(column, out var newVal);

                bool valueChanged = oldVal != newVal;

                if (valueChanged)
                {
                    if (oldVal != null)
                        index.RemoveFromIndex(oldVal, oldRow);

                    if (newVal != null)
                        index.AddToIndex(newVal, newRow);
                }
                else if (newVal != null)
                {
                    // Refresh row reference even if key didn't change
                    index.RemoveFromIndex(newVal, oldRow);
                    index.AddToIndex(newVal, newRow);
                }
            }
        }
        public void UpdateIndexesForDelete(Dictionary<string, string> row)
        {
            foreach (var kvp in Indexes)
            {
                var column = kvp.Key;
                var index = kvp.Value;

                if (row.TryGetValue(column, out var value))
                {
                    index.RemoveFromIndex(value, row);
                }
            }
        }
        public void UpdateIndexesForInsert(Dictionary<string, string> row)
        {
            foreach (var kvp in Indexes)
            {
                var column = kvp.Key;
                var index = kvp.Value;

                if (row.TryGetValue(column, out var value))
                {
                    index.AddToIndex(value, row);
                }
            }
        }

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

            foreach (var col in Columns)
            {
                if (Indexes.TryGetValue(col.Name, out var index))
                {
                    index.AddToIndex(finalRow[col.Name], finalRow);  // ✅ Update existing index
                }
            }

            Console.WriteLine("row inserted Successfully");
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
                // Check if primary key is being updated
                if (updatedValues.ContainsKey(primaryKeyColumn.Name))
                {
                    string newPrimaryKeyValue = updatedValues[primaryKeyColumn.Name];

                    // Find tables that reference this table as a foreign key
                    foreach (var otherTable in ParentDatabase.Tables.Values)
                    {
                        foreach (var column in otherTable.Columns)
                        {
                            if (column.Constraint.Has(ConstraintType.ForeignKey) &&
                                column.Constraint.ReferenceTable == this.Name &&
                                column.Constraint.ReferenceColumn == primaryKeyColumn.Name)
                            {
                                bool isReferenced = otherTable.Rows.Any(r => r[col.Name] == primaryKeyValue);
                                if (isReferenced)
                                {
                                    throw new Exception($"Primary key '{primaryKeyColumn.Name}' with value '{primaryKeyValue}' is referenced in table '{otherTable.Name}'. Update not allowed.");
                                }
                            }
                        }
                    }
                }


                if (Indexes.TryGetValue(col.Name, out var index))
                {
                    var oldValue = targetRow[col.Name];
                    index.RemoveFromIndex(oldValue, targetRow);
                }

                // Now update
                targetRow[col.Name] = value;

                if (index != null)
                {
                    index.AddToIndex(value, targetRow);
                }



            }

            Console.WriteLine("Row updated successfully.");
        }

        // Delete a row from the table and cascade delete as necessary
        public void DeleteRow(string primaryKeyValue, TransactionManager tm, Guid? deletingTransactionId = null)
        {
            
            if (ParentDatabase == null)
                throw new Exception("Parent database reference is required for constraint validation.");

            var primaryKeyColumn = Columns.FirstOrDefault(c => c.Constraint.Has(ConstraintType.PrimaryKey));
            if (primaryKeyColumn == null)
                throw new Exception("No primary key defined for the table.");

            var rowToDelete = Rows.FirstOrDefault(r => r[primaryKeyColumn.Name] == primaryKeyValue);
            if (rowToDelete == null)
                throw new Exception($"No row found with {primaryKeyColumn.Name} = {primaryKeyValue}");

            int rowIndex = Rows.FindIndex(r => r[primaryKeyColumn.Name] == primaryKeyValue);
           
            // 🟢 If not inside a transaction
            if (deletingTransactionId == null || deletingTransactionId == Guid.Empty)
            {
                
                // Cascading delete
                foreach (var childTable in ParentDatabase.Tables.Values)
                {
                    foreach (var col in childTable.Columns)

                    {
                        if (col.Constraint.Has(ConstraintType.ForeignKey) &&
                            col.Constraint.ReferenceTable == Name &&
                            col.Constraint.ReferenceColumn == primaryKeyColumn.Name)
                        {
                            var childRowsToDelete = childTable.Rows
                                .Where(r => r.ContainsKey(col.Name) && r[col.Name] == primaryKeyValue)
                                .ToList();

                            foreach (var childRow in childRowsToDelete)
                            {
                                var childPk = childTable.Columns.First(c => c.Constraint.Has(ConstraintType.PrimaryKey)).Name;
                                childTable.DeleteRow(childRow[childPk], tm, null);
                                Console.WriteLine($"Cascading delete: Immediately deleted from '{childTable.Name}' where '{col.Name}' = {primaryKeyValue}");
                            }
                        }
                    }
                }

                // Remove from indexes
                foreach (var index in Indexes.Values)
                {
                    var indexColumn = index.ColumnName;
                    if (rowToDelete.ContainsKey(indexColumn))
                    {
                        var key = (IComparable)rowToDelete[indexColumn];
                        index.RemoveFromIndex(key, rowToDelete);
                    }
                }

                // Final removal
                Rows.Remove(rowToDelete);
                Console.WriteLine("Row deleted immediately (no transaction).");
                return;
            }

            // 🔁 Stack-based cascading delete with lock-safe behavior
            var deleteQueue = new Stack<(Table table, string pkValue)>();
            var affectedRows = new List<(Table table, int rowIdx, Dictionary<string, string> rowCopy)>();

            deleteQueue.Push((this, primaryKeyValue));

            while (deleteQueue.Count > 0)
            {
                var (currentTable, currentPkVal) = deleteQueue.Pop();
                var currentPkCol = currentTable.Columns.First(c => c.Constraint.Has(ConstraintType.PrimaryKey)).Name;
                var currentRow = currentTable.Rows.FirstOrDefault(r => r[currentPkCol] == currentPkVal);
                if (currentRow == null) continue;

                int currentRowIdx = currentTable.Rows.FindIndex(r => r[currentPkCol] == currentPkVal);
                if (currentRowIdx == -1) continue;

                // Defer concurrency and lock checks until after row discovery
                affectedRows.Add((currentTable, currentRowIdx, new Dictionary<string, string>(currentRow)));

                // Enqueue children to delete
                foreach (var childTable in currentTable.ParentDatabase.Tables.Values)
                {
                    foreach (var col in childTable.Columns)
                    {
                        if (col.Constraint.Has(ConstraintType.ForeignKey) &&
                            col.Constraint.ReferenceTable == currentTable.Name &&
                            col.Constraint.ReferenceColumn == currentPkCol)
                        {
                            var childRows = childTable.Rows
                                .Where(r => r.ContainsKey(col.Name) && r[col.Name] == currentPkVal)
                                .ToList();

                            foreach (var cr in childRows)
                            {
                                var childPk = childTable.Columns.First(c => c.Constraint.Has(ConstraintType.PrimaryKey)).Name;
                                deleteQueue.Push((childTable, cr[childPk]));
                                Console.WriteLine($"Cascading delete: Deferred delete for '{childTable.Name}' where '{col.Name}' = {currentPkVal}");
                            }
                        }
                    }
                }
            }

            // 🔒 Acquire locks in deterministic order (by table name, then row index)
            var lockOrder = affectedRows.OrderBy(t => t.table.Name).ThenBy(t => t.rowIdx).ToList();

            foreach (var (table, rowIdx, _) in lockOrder)
            {
                try
                {
                    tm.concurrencyControl.AcquireWriteLock(table.Name, rowIdx, deletingTransactionId.Value);
                }
                catch (TimeoutException ex)
                {
                    Console.WriteLine($"Timeout acquiring lock on {table.Name}[{rowIdx}]: {ex.Message}");
                    throw;
                }
            }

            // 🔧 Perform deletion and log
            foreach (var (table, rowIdx, rowData) in lockOrder)
            {
                tm.LogOperation(deletingTransactionId.Value, "delete", table.Name,
                    new List<Dictionary<string, string>> { rowData }, null, table.ParentDatabase);

                tm.concurrencyControl.MarkRowAsDeleted(table.Name, rowIdx, deletingTransactionId.Value);

                // Remove from indexes
                //foreach (var index in table.Indexes.Values)
                //{
                //    var indexColumn = index.ColumnName;
                //    if (rowData.ContainsKey(indexColumn))
                //    {
                //        var key = (IComparable)rowData[indexColumn];
                //        index.RemoveFromIndex(key, rowData);
                //    }
                //}

                Console.WriteLine($"Deferred delete logged for {table.Name} where {table.Columns.First(c => c.Constraint.Has(ConstraintType.PrimaryKey)).Name} = {rowData[table.Columns.First(c => c.Constraint.Has(ConstraintType.PrimaryKey)).Name]}");
            }
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
