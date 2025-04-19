 class Transaction
{
    public Guid Id { get; }
    public List<(string Operation, string TableName, List<Dictionary<string, string>> Before, List<Dictionary<string, string>> After)> Operations { get; }
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

        if (operation == "insert" && after != null)
        {
            foreach (var row in after)
            {
                transaction.InsertedRows.Add((tableName, row));
            }
        }

        // Acquire write locks
        if (db.Tables.TryGetValue(tableName, out var table))
        {
            foreach (var row in after ?? Enumerable.Empty<Dictionary<string, string>>())
            {
                int rowIndex = table.Rows.FindIndex(r => AreRowsEqual(r, row));
                concurrencyControl.AcquireWriteLock(tableName, rowIndex == -1 ? -1 : rowIndex, transactionId);
            }
        }

        transaction.Operations.Add((operation, tableName, before, after));
    }

    public List<Dictionary<string, string>> GetVisibleRows(Guid transactionId, string tableName, Database db)
    {
        if (!db.Tables.ContainsKey(tableName)) return new List<Dictionary<string, string>>();

        var table = db.Tables[tableName];
        var visibleRows = new List<Dictionary<string, string>>();

        foreach (var row in table.Rows)
        {
            // Basic logic: show all committed rows and this transaction's uncommitted changes
            bool isVisible = true;

            foreach (var t in transactions.Values)
            {
                if (t.Id == transactionId || !t.IsActive) continue;

                foreach (var op in t.Operations)
                {
                    if (op.TableName == tableName && op.Operation == "insert")
                    {
                        foreach (var afterRow in op.After)
                        {
                            if (AreRowsEqual(afterRow, row))
                            {
                                isVisible = false; // Row was inserted by another *active* transaction
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

        foreach (var (operation, tableName, beforeList, afterList) in transaction.Operations)
        {
            if (!database.Tables.ContainsKey(tableName)) continue;

            var table = database.Tables[tableName];

            if (operation == "insert")
            {
                foreach (var afterRow in afterList)
                    table.Rows.RemoveAll(r => AreRowsEqual(r, afterRow));
            }
            else if (operation == "delete")
            {
                table.Rows.AddRange(beforeList);
            }
            else if (operation == "update")
            {
                for (int i = 0; i < beforeList.Count; i++)
                {
                    var oldRow = beforeList[i];
                    var newRow = afterList[i];
                    var rowToUpdate = table.Rows.FirstOrDefault(r => AreRowsEqual(r, newRow));
                    if (rowToUpdate != null)
                    {
                        foreach (var key in oldRow.Keys)
                            rowToUpdate[key] = oldRow[key];
                    }
                }
            }
        }

        // Also remove uncommitted inserted rows
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
