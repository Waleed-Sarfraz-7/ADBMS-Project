class Program
{
    static void Main()
    {
        Database db = StorageManager.LoadDatabase();
        TransactionManager tm = new TransactionManager();
        QueryProcessor qp = new QueryProcessor(db, tm);

        Console.WriteLine("SQL> ");
        while (true)
        {
            string query = Console.ReadLine();
            if (query.ToLower() == "exit") break;
            qp.ExecuteQuery(query);
        }

        StorageManager.SaveDatabase(db);
    }
}
