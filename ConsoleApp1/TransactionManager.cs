 class TransactionManager
{
    private Stack<List<(string Operation, string TableName, List<Dictionary<string, string>> Before, List<Dictionary<string, string>> After)>> transactionStack;
    private bool inTransaction;

    public bool IsInTransaction() => inTransaction;

    public TransactionManager()
    {
        transactionStack = new Stack<List<(string, string, List<Dictionary<string, string>>, List<Dictionary<string, string>>)>>();
        inTransaction = false;
    }

    public void BeginTransaction()
    {
        if (inTransaction)
        {
            Console.WriteLine("Transaction already in progress.");
            return;
        }

        inTransaction = true;
        transactionStack.Push(new List<(string, string, List<Dictionary<string, string>>, List<Dictionary<string, string>>)>());
        Console.WriteLine("Transaction Started...");
    }

    public void LogOperation(string operation, string tableName, List<Dictionary<string, string>> before, List<Dictionary<string, string>> after)
    {
        if (!inTransaction) return;
        transactionStack.Peek().Add((operation, tableName, before, after));
    }

    public void CommitTransaction()
    {
        if (!inTransaction)
        {
            Console.WriteLine("No transaction to commit.");
            return;
        }

        transactionStack.Pop();
        inTransaction = false;
        Console.WriteLine("Transaction Committed Successfully.");
    }

    public void RollbackTransaction(Database database)
    {
        if (!inTransaction)
        {
            Console.WriteLine("No transaction to rollback.");
            return;
        }

        var operations = transactionStack.Pop();
        operations.Reverse();

        foreach (var (operation, tableName, beforeList, afterList) in operations)
        {
            if (!database.Tables.ContainsKey(tableName)) continue;

            var table = database.Tables[tableName];

            if (operation == "insert")
            {
                // remove inserted rows
                foreach (var afterRow in afterList)
                    table.Rows.RemoveAll(r => AreRowsEqual(r, afterRow));
            }
            else if (operation == "delete")
            {
                // re-insert deleted rows
                table.Rows.AddRange(beforeList);
            }
            else if (operation == "update")
            {
                // revert all updated rows
                foreach (var i in Enumerable.Range(0, beforeList.Count))
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

        inTransaction = false;
        Console.WriteLine("Transaction Rolled Back Successfully.");
    }

    private bool AreRowsEqual(Dictionary<string, string> r1, Dictionary<string, string> r2)
    {
        return r1.Count == r2.Count && r1.All(kv => r2.ContainsKey(kv.Key) && r2[kv.Key] == kv.Value);
    }


}
