using System.Data;
using System.Threading;

namespace ConsoleApp1
{
    class Transaction
    {
        public Guid Id { get; }
        public List<OperationLog> Operations { get; }
        public bool IsActive { get; set; }
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
        private readonly object transactionLock = new();  // For thread-safety

        public ConcurrencyControl concurrencyControl;

        public TransactionManager(ConcurrencyControl control)
        {
            transactions = new();
            concurrencyControl = control;
        }

        public Dictionary<Guid, Transaction> GetAllTransactions()
        {
            lock (transactionLock)
            {
                return new Dictionary<Guid, Transaction>(transactions);
            }
        }
        public bool AnyActiveTransaction()
        {
            return transactions.Values.Any(t => t.IsActive);
        }


        public Guid BeginTransaction()
        {
            var transaction = new Transaction();
            lock (transactionLock)
            {
                transactions[transaction.Id] = transaction;
            }
            Console.WriteLine($"Transaction Started... ID: {transaction.Id}");
            return transaction.Id;
        }



        public void LogOperation(Guid transactionId, string operation, string tableName,
    List<Dictionary<string, string>> before, List<Dictionary<string, string>> after, Database db)
        {
            Transaction transaction;

            lock (transactionLock)
            {
                if (!transactions.TryGetValue(transactionId, out transaction) || !transaction.IsActive)
                    return;
            }

            var beforeWithIndex = new List<(int, Dictionary<string, string>)>();
            var afterWithIndex = new List<(int, Dictionary<string, string>)>();
            Table? tempDDLTable = null;

            if (operation == "create")
            {
                if (db.Tables.TryGetValue(tableName, out var createdTable))
                    tempDDLTable = createdTable;

                transaction.Operations.Add(new OperationLog
                {
                    Operation = operation,
                    TableName = tableName,
                    TempTableForDDL = tempDDLTable
                });

                return;
            }

            if (!db.Tables.TryGetValue(tableName, out var table))
                return;

            var pkCol = table.Columns.First(c => c.Constraint.Has(ConstraintType.PrimaryKey));
            var alreadyLocked = new HashSet<int>();

            // Handle the 'before' operation
            if (before != null)
            {
                foreach (var row in before)
                {
                    string pkVal = row[pkCol.Name];
                    int index = table.Rows.FindIndex(r => r[pkCol.Name] == pkVal);
                    if (index == -1)
                        throw new Exception($"Row with {pkCol.Name} = {pkVal} not found.");

                    if (!alreadyLocked.Contains(index))
                    {
                        // If the row is being updated or deleted, acquire a write lock
                        if (operation == "update" || operation == "delete")
                        {
                            concurrencyControl.AcquireWriteLock(tableName, index, transactionId);
                        }
                        else
                        {
                            // If no write lock, acquire an upgradeable read lock
                            concurrencyControl.AcquireUpgradeableReadLock(tableName, index, transactionId);
                        }

                        alreadyLocked.Add(index);
                    }

                    // 🔁 Refresh row after acquiring the lock
                    var lockedRow = new Dictionary<string, string>(table.Rows[index]);
                    beforeWithIndex.Add((index, lockedRow));
                }
            }

            // Handle the 'after' operation
            if (after != null)
            {
                foreach (var row in after)
                {
                    string pkVal = row[pkCol.Name];
                    int index = table.Rows.FindIndex(r => r[pkCol.Name] == pkVal);

                    if (!alreadyLocked.Contains(index))
                    {
                        // If the row is being updated or inserted, acquire a write lock
                        if (operation == "insert" || operation == "update")
                        {
                            concurrencyControl.AcquireWriteLock(tableName, index, transactionId);
                        }
                        else
                        {
                            // If no write lock, acquire an upgradeable read lock
                            concurrencyControl.AcquireUpgradeableReadLock(tableName, index, transactionId);
                        }

                        alreadyLocked.Add(index);
                    }

                    afterWithIndex.Add((index, row));

                    if (operation == "insert")
                        transaction.InsertedRows.Add((tableName, row));
                }
            }

            // Log the operation with before and after states
            transaction.Operations.Add(new OperationLog
            {
                Operation = operation,
                TableName = tableName,
                Before = beforeWithIndex,
                After = afterWithIndex
            });
        }


        public List<Dictionary<string, string>> GetVisibleRows(Guid transactionId, string tableName, Database db)
        {
            var visible = new List<Dictionary<string, string>>();

            if (!db.Tables.TryGetValue(tableName, out var table))
                return visible;

            lock (transactionLock)
            {
                foreach (var row in table.Rows)
                {
                    bool visibleToTransaction = true;

                    foreach (var t in transactions.Values)
                    {
                        if (t == null || t.Id == transactionId || !t.IsActive) continue;

                        foreach (var op in t.Operations)
                        {
                            if (op.TableName != tableName) continue;

                            if (op.Operation == "delete" && op.Before.Any(r => AreRowsEqual(r.Item2, row)) ||
                                op.Operation == "update" && op.After.Any(r => AreRowsEqual(r.Item2, row)))
                            {
                                visibleToTransaction = false;
                                break;
                            }
                        }

                        if (!visibleToTransaction)
                            break;
                    }

                    if (visibleToTransaction)
                        visible.Add(row);
                }
            }

            return visible;
        }

        public void CommitTransaction(Guid transactionId, Database db)
        {
            Transaction transaction = null;

            lock (transactionLock)
            {
                if (!transactions.TryGetValue(transactionId, out transaction) || !transaction.IsActive)
                    return;

                transaction.IsActive = false;
                transactions.Remove(transactionId);
            }

            foreach (var op in transaction.Operations)
            {
                if (!db.Tables.TryGetValue(op.TableName, out var table)) continue;
                var pkCol = table.Columns.FirstOrDefault(c => c.Constraint.Has(ConstraintType.PrimaryKey));
                if (pkCol == null) continue;

                if (op.Operation == "delete")
                {
                    foreach (var (_, row) in op.Before)
                    {
                        table.UpdateIndexesForDelete(row); // 🧠 Remove from index
                        table.Rows.RemoveAll(r => r[pkCol.Name] == row[pkCol.Name]);
                    }
                }
                else if (op.Operation == "update")
                {
                    for (int i = 0; i < op.Before.Count; i++)
                    {
                        var (index, oldRow) = op.Before[i];
                        var (_, newRow) = op.After[i];
                        table.UpdateIndexesForUpdate(oldRow, newRow); // 🔁 Update index
                        foreach (var kv in newRow)
                            table.Rows[index][kv.Key] = kv.Value;
                    }
                }
                else if (op.Operation == "insert")
                {
                    foreach (var (_, row) in op.After)
                    {
                        if (!table.Rows.Any(r => r[pkCol.Name] == row[pkCol.Name]))
                        {
                            table.Rows.Add(new Dictionary<string, string>(row));
                            table.UpdateIndexesForInsert(row); // ➕ Add to index
                        }
                    }
                }
            }

            concurrencyControl.ReleaseLocks(transactionId);
            Console.WriteLine($"Transaction {transactionId} committed successfully.");
        }


        public void RollbackTransaction(Guid transactionId, Database db)
        {
            Transaction transaction;
            lock (transactionLock)
            {
                if (!transactions.TryGetValue(transactionId, out transaction) || !transaction.IsActive)
                    return;

                transaction.IsActive = false;
                transactions.Remove(transactionId);
            }

            transaction.Operations.Reverse();

            foreach (var op in transaction.Operations)
            {
                if (op.Operation == "create" && op.TempTableForDDL != null)
                {
                    db.Tables.Remove(op.TableName);
                    Console.WriteLine($"Rolled back table creation: {op.TableName}");
                    continue;
                }

                if (!db.Tables.TryGetValue(op.TableName, out var table)) continue;

                if (op.Operation == "insert")
                {
                    foreach (var (_, row) in op.After)
                    {
                        table.UpdateIndexesForDelete(row); // ⬅️ Remove inserted row from index
                        table.Rows.RemoveAll(r => AreRowsEqual(r, row));
                    }
                }
                else if (op.Operation == "delete")
                {
                    foreach (var (idx, row) in op.Before)
                    {
                        table.Rows.Insert(idx, row);
                        table.UpdateIndexesForInsert(row); // ⬅️ Restore row to index
                    }
                }
                else if (op.Operation == "update")
                {
                    for (int i = 0; i < op.Before.Count; i++)
                    {
                        var (idx, oldRow) = op.Before[i];
                        var (_, newRow) = op.After[i];
                        table.UpdateIndexesForUpdate(newRow, oldRow); // ⬅️ Reverse the update
                        foreach (var key in oldRow.Keys)
                            table.Rows[idx][key] = oldRow[key];
                    }
                }
            }

            foreach (var (tableName, row) in transaction.InsertedRows)
            {
                if (db.Tables.TryGetValue(tableName, out var table))
                {
                    table.UpdateIndexesForDelete(row); // Extra safety
                    table.Rows.RemoveAll(r => AreRowsEqual(r, row));
                }
            }

            concurrencyControl.ReleaseLocks(transactionId);
            Console.WriteLine($"Transaction {transactionId} rolled back successfully.");
        }


        private bool AreRowsEqual(Dictionary<string, string> r1, Dictionary<string, string> r2)
        {
            return r1.Count == r2.Count && r1.All(kv => r2.TryGetValue(kv.Key, out var val) && val == kv.Value);
        }
    }
}
