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

            // 🟢 If NOT in a transaction, delete immediately and apply cascading deletes
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
                                string childPk = childTable.Columns
                                    .First(c => c.Constraint.Has(ConstraintType.PrimaryKey)).Name;

                                childTable.DeleteRow(childRow[childPk], tm, null);  // Pass null again
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

                // Remove the row directly
                Rows.Remove(rowToDelete);
                Console.WriteLine("Row deleted immediately (no transaction).");
                return;
            }


            // 🔁 Perform stack-based delete (to avoid recursion)
            var deleteQueue = new Stack<(Table table, string pkValue)>();
            deleteQueue.Push((this, primaryKeyValue));

            while (deleteQueue.Count > 0)
            {
                var (currentTable, currentPkVal) = deleteQueue.Pop();
                var currentPrimaryKey = currentTable.Columns.First(c => c.Constraint.Has(ConstraintType.PrimaryKey)).Name;
                var rowToDel = currentTable.Rows.FirstOrDefault(r => r[currentPrimaryKey] == currentPkVal);
                if (rowToDel == null) continue;
                var rowIdx = currentTable.Rows.FindIndex(r => r[currentPrimaryKey] == currentPkVal);

                // Locking for transaction
                foreach (var kvp in tm.GetAllTransactions())
                {
                    var txId = kvp.Key;
                    var tx = kvp.Value;
                    if (tx.IsActive && txId != deletingTransactionId)
                    {
                        tm.concurrencyControl.AcquireReadLock(currentTable.Name, rowIdx, txId);
                    }
                }

                // Add children to delete queue
                foreach (var child in currentTable.ParentDatabase.Tables.Values)
                {
                    foreach (var col in child.Columns)
                    {
                        if (col.Constraint.Has(ConstraintType.ForeignKey) &&
                            col.Constraint.ReferenceTable == currentTable.Name &&
                            col.Constraint.ReferenceColumn == currentPrimaryKey)
                        {
                            var matchingChildRows = child.Rows
                                .Where(r => r.ContainsKey(col.Name) && r[col.Name] == currentPkVal)
                                .ToList();

                            foreach (var cr in matchingChildRows)
                            {
                                var childPk = child.Columns.First(c => c.Constraint.Has(ConstraintType.PrimaryKey)).Name;
                                deleteQueue.Push((child, cr[childPk]));
                                Console.WriteLine($"Cascading delete: Deferred delete for '{child.Name}' where '{col.Name}' = {currentPkVal}");
                            }
                        }
                    }
                }

                // Log delete
                var beforeData = new List<Dictionary<string, string>> { new(rowToDel) };
                tm.LogOperation(deletingTransactionId.Value, "delete", currentTable.Name, beforeData, null, currentTable.ParentDatabase);
                tm.concurrencyControl.MarkRowAsDeleted(currentTable.Name, rowIdx, deletingTransactionId.Value);

                
                // Remove from indexes
                foreach (var index in currentTable.Indexes.Values)
                {
                    var indexColumn = index.ColumnName;
                    if (rowToDel.ContainsKey(indexColumn))
                    {
                        var key = (IComparable)rowToDel[indexColumn];
                        index.RemoveFromIndex(key, rowToDel);
                    }
                }


                Console.WriteLine($"Deferred delete logged for {currentTable.Name} where {currentPrimaryKey} = {currentPkVal}");
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
