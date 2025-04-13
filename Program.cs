using ConsoleApp1;

class Program
{
    static void Main()
    {
        //DBMS dBMS = StorageManager.LoadDBMS();
        //TransactionManager tm = new TransactionManager();
        //QueryProcessor qp = new QueryProcessor( tm,dBMS);

        //Console.WriteLine("SQL> ");
        //while (true)
        //{
        //    if (qp.GetQPDatabase() == null)
        //    {
        //        Console.WriteLine("You must select a database first");
        //    }
        //    string query = Console.ReadLine();
        //    if (query.ToLower() == "exit") break;
        //    qp.ExecuteQuery(query);
        //    if (!tm.IsInTransaction())
        //    {
        //        StorageManager.SaveDBMS(dBMS);
        //    }

        //}


        var dbms = new DBMS();
        dbms.CreateDatabase("School");
        dbms.UseDatabase("School");

        var columns = new List<Column>
{
    new Column("id", "INT"),
    new Column("name", "STRING", maxLength: 20),
    new Column("isStudent", "BOOLEAN")
};

        var db = dbms.GetCurrentDatabase();
        db.CreateTable("Students", columns);

        var row1 = new Dictionary<string, string>
{
    { "id", "1" },
    { "name", "Ali" },
    { "isStudent", "true" }
};

        db.Tables["Students"].InsertRow(row1);
        db.Tables["Students"].DisplayTable();



    }
}
