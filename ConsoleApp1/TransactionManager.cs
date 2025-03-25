class TransactionManager
{
    private Stack<List<(string, Dictionary<string, string>)>> transactionStack;
    private bool inTransaction;

    public TransactionManager()
    {
        transactionStack = new Stack<List<(string, Dictionary<string, string>)>>();
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
        transactionStack.Push(new List<(string, Dictionary<string, string>)>());
        Console.WriteLine("Transaction Started...");
    }

    public void LogOperation(string tableName, Dictionary<string, string> row)
    {
        if (inTransaction)
        {
            transactionStack.Peek().Add((tableName, new Dictionary<string, string>(row)));
        }
    }

    public void CommitTransaction()
    {
        if (!inTransaction)
        {
            Console.WriteLine("No transaction to commit.");
            return;
        }

        transactionStack.Pop(); // Clear transaction log
        inTransaction = false;
        Console.WriteLine("Transaction Committed.");
    }

    public void RollbackTransaction(Database database)
    {
        if (!inTransaction)
        {
            Console.WriteLine("No transaction to rollback.");
            return;
        }

        List<(string, Dictionary<string, string>)> operations = transactionStack.Pop();
        foreach (var operation in operations)
        {
            string tableName = operation.Item1;
            Dictionary<string, string> row = operation.Item2;

            if (database.Tables.ContainsKey(tableName))
            {
                database.Tables[tableName].Rows.RemoveAll(r => r["ID"] == row["ID"]);
            }
        }

        inTransaction = false;
        Console.WriteLine("Transaction Rolled Back.");
    }
    public bool IsInTransaction()
    {
        return inTransaction;
    }
}
