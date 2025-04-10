using ConsoleApp1;

class Program
{
    static void Main()
    {
        DBMS dBMS = StorageManager.LoadDBMS();
        TransactionManager tm = new TransactionManager();
        QueryProcessor qp = new QueryProcessor( tm,dBMS);
        
        Console.WriteLine("SQL> ");
        while (true)
        {
            if (qp.GetQPDatabase() == null)
            {
                Console.WriteLine("You must select a database first");
            }
            string query = Console.ReadLine();
            if (query.ToLower() == "exit") break;
            qp.ExecuteQuery(query);
            if (!tm.IsInTransaction())
            {
                StorageManager.SaveDBMS(dBMS);
            }
            
        }
        

    }
}
