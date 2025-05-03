
namespace ConsoleApp1
{
    class Transaction
    {
        public Guid Id { get; }
        public List<OperationLog> Operations { get; }
        public bool IsActive { get; set; }

        // Track inserted rows per transaction
        public List<(string TableName, Dictionary<string, string> Row)> InsertedRows { get; }

        public Transaction()
        {
            Id = Guid.NewGuid();
            Operations = new();
            IsActive = true;
            InsertedRows = new();
        }
    }
    class OperationLog
    {
        public string Operation { get; set; }
        public string TableName { get; set; }
        public List<(int RowIndex, Dictionary<string, string> Row)> Before { get; set; }
        public List<(int RowIndex, Dictionary<string, string> Row)> After { get; set; }
        public Table? TempTableForDDL;
    }

    class TransactionManager
    {
        private readonly Dictionary<Guid, Transaction> transactions;
        private readonly ConcurrencyControl concurrencyControl;

        public TransactionManager(ConcurrencyControl control)
        {
            transactions = new();
            concurrencyControl = control;
        }

        public Guid BeginTransaction()
        {
            var transaction = new Transaction();
            transactions[transaction.Id] = transaction;
            Console.WriteLine($"Transaction Started... ID: {transaction.Id}");
            return transaction.Id;
        }

        public void LogOperation(Guid transactionId, string operation, string tableName,
            List<Dictionary<string, string>> before, List<Dictionary<string, string>> after, Database db)
        {
            if (!transactions.TryGetValue(transactionId, out var transaction) || !transaction.IsActive) return;

            var beforeWithIndex = new List<(int, Dictionary<string, string>)>();
            var afterWithIndex = new List<(int, Dictionary<string, string>)>();
            Table? tempDDLTable = null;

            if (operation == "create")
            {
                if (db.Tables.TryGetValue(tableName, out var createdTable))
                {
                    tempDDLTable = createdTable; // Save the table snapshot for rollback
                }

                transaction.Operations.Add(new OperationLog
                {
                    Operation = operation,
                    TableName = tableName,
                    TempTableForDDL = tempDDLTable
                });

                return;
            }


            if (db.Tables.TryGetValue(tableName, out var table))
            {
                if (before != null)
                {
                    foreach (var row in before)
                    {
                        int index = table.Rows.FindIndex(r => AreRowsEqual(r, row));
                        beforeWithIndex.Add((index, row));

                        // Lock for delete and update
                        if (operation == "delete" || operation == "update")
                            concurrencyControl.AcquireWriteLock(tableName, index, transactionId);
                    }
                }

                if (after != null)
                {
                    foreach (var row in after)
                    {
                        int index = table.Rows.FindIndex(r => AreRowsEqual(r, row));

                        if (operation == "insert")
                            transaction.InsertedRows.Add((tableName, row));

                        concurrencyControl.AcquireWriteLock(tableName, index == -1 ? -1 : index, transactionId);
                        afterWithIndex.Add((index, row));
                    }
                }

                transaction.Operations.Add(new OperationLog
                {
                    Operation = operation,
                    TableName = tableName,
                    Before = beforeWithIndex,
                    After = afterWithIndex
                });
            }
        }

        public List<Dictionary<string, string>> GetVisibleRows(Guid transactionId, string tableName, Database db)
        {
            if (!db.Tables.ContainsKey(tableName)) return new List<Dictionary<string, string>>();
            var table = db.Tables[tableName];
            var visibleRows = new List<Dictionary<string, string>>();

            foreach (var row in table.Rows)
            {
                bool isVisible = true;

                foreach (var t in transactions.Values)
                {
                    if (t.Id == transactionId || !t.IsActive) continue;

                    foreach (var op in t.Operations)
                    {
                        if (op.TableName == tableName && op.Operation == "insert")
                        {
                            foreach (var (_, afterRow) in op.After)
                            {
                                if (AreRowsEqual(afterRow, row))
                                {
                                    isVisible = false;
                                    break;
                                }
                            }
                        }
                    }

                    if (!isVisible) break;
                }

                if (isVisible)
                    visibleRows.Add(row);
            }

            return visibleRows;
        }

        public void CommitTransaction(Guid transactionId)
        {
            if (!transactions.TryGetValue(transactionId, out var transaction) || !transaction.IsActive)
            {
                Console.WriteLine("No active transaction to commit.");
                return;
            }

            transaction.IsActive = false;
            transactions.Remove(transactionId);
            concurrencyControl.ReleaseLocks(transactionId);
            Console.WriteLine($"Transaction {transactionId} committed successfully.");
        }

        public void RollbackTransaction(Guid transactionId, Database database)
        {
            if (!transactions.TryGetValue(transactionId, out var transaction) || !transaction.IsActive)
            {
                Console.WriteLine("No active transaction to rollback.");
                return;
            }

            transaction.Operations.Reverse();

            foreach (var op in transaction.Operations)
            {
                if (op.Operation == "create" && op.TempTableForDDL != null)
                {
                    // Rollback table creation
                    if (database.Tables.ContainsKey(op.TableName))
                    {
                        database.Tables.Remove(op.TableName);
                        Console.WriteLine($"Rolled back table creation: {op.TableName}");
                    }

                    continue;
                }

                if (!database.Tables.ContainsKey(op.TableName)) continue;
                var table = database.Tables[op.TableName];

                if (op.Operation == "insert")
                {
                    foreach (var (_, afterRow) in op.After)
                        table.Rows.RemoveAll(r => AreRowsEqual(r, afterRow));
                }
                else if (op.Operation == "delete")
                {
                    foreach (var (rowIndex, beforeRow) in op.Before)
                    {
                        if (rowIndex < 0 || rowIndex > table.Rows.Count)
                            table.Rows.Add(beforeRow);
                        else
                            table.Rows.Insert(rowIndex, beforeRow);
                    }
                }
                else if (op.Operation == "update")
                {
                    foreach (var (rowIndex, beforeRow) in op.Before)
                    {
                        if (rowIndex >= 0 && rowIndex < table.Rows.Count)
                        {
                            foreach (var key in beforeRow.Keys)
                                table.Rows[rowIndex][key] = beforeRow[key];
                        }
                    }
                }
            }

            foreach (var (tableName, row) in transaction.InsertedRows)
            {
                if (database.Tables.TryGetValue(tableName, out var table))
                    table.Rows.RemoveAll(r => AreRowsEqual(r, row));
            }

            transaction.IsActive = false;
            transactions.Remove(transactionId);
            concurrencyControl.ReleaseLocks(transactionId);
            Console.WriteLine($"Transaction {transactionId} rolled back successfully.");
        }

        private bool AreRowsEqual(Dictionary<string, string> r1, Dictionary<string, string> r2)
        {
            return r1.Count == r2.Count && r1.All(kv => r2.ContainsKey(kv.Key) && r2[kv.Key] == kv.Value);
        }
    }

}
