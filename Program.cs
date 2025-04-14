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


        // var dbms = new DBMS();
        // dbms.CreateDatabase("School");
        // dbms.UseDatabase("School");
        //
        // var columns = new List<Column>
        //{
        //  new Column("id", "INT"),
        //  new Column("name", "STRING", maxLength: 20),
        //  new Column("isStudent", "BOOLEAN")
        //};

        //   var db = dbms.GetCurrentDatabase();
        // db.CreateTable("Students", columns);

        //  var row1 = new Dictionary<string, string>
        //{
        //  { "id", "1" },
        //  { "name", "Ali" },
        //  { "isStudent", "true" }
        //};

        //      db.Tables["Students"].InsertRow(row1);
        //    db.Tables["Students"].DisplayTable();
        var db = new MVCCDatabase();
        db.CreateTable("users");

        var cc = new ConcurrencyControl();
        var tm = new TransactionManager(db,cc);

        // TX1 inserts
        var tx1 = tm.BeginTransaction();
        tm.Insert(tx1, "users", new Dictionary<string, string> { ["id"] = "1", ["name"] = "Alice" });

        // TX2 tries to read - should not see Alice
        var tx2 = tm.BeginTransaction();
        var data2 = tm.Read(tx2, "users");
        Console.WriteLine($"TX2 sees {data2.Count} rows."); // 0 rows

        // TX1 commits
        tm.Commit(tx1);
        // TX2 reads again - now sees 1 as the transaction is commited tx1
        var stillInvisible = tm.Read(tx2, "users");
        Console.WriteLine($"TX2 still sees {stillInvisible.Count} rows.");

        // New transaction TX3 sees it
        var tx3 = tm.BeginTransaction();
        var data3 = tm.Read(tx3, "users");
        Console.WriteLine($"TX3 sees {data3.Count} rows."); // 1 row



    }
}
