using ConsoleApp1;
using static System.Runtime.InteropServices.JavaScript.JSType;

class Program
{
    static void Main()
    {
        DBMS dBMS = StorageManager.LoadDBMS();
        var cc = new ConcurrencyControl();
        TransactionManager tm = new TransactionManager(cc);
        QueryProcessor qp = new QueryProcessor(tm, dBMS);

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
           
           
            

        }


        //        var dbms = StorageManager.LoadDBMS();
        //        var cc = new ConcurrencyControl();
        //        var tm =  new TransactionManager(cc);
        //        dbms.UseDatabase("School");
        //        var db = dbms.GetCurrentDatabase();
        //        var departmentColumns = new List<Column>
        //{
        //    new Column("id", "INT", constraint: new ColumnConstraint
        //    {
        //        Constraints = ConstraintType.PrimaryKey | ConstraintType.NotNull
        //    }),
        //    new Column("name", "STRING", maxLength: 50)
        //};
        //        db.CreateTable("Departments", departmentColumns);




        //        var studentColumns = new List<Column>
        //{
        //    new Column("id", "INT", constraint: new ColumnConstraint
        //    {
        //        Constraints = ConstraintType.PrimaryKey | ConstraintType.NotNull
        //    }),
        //    new Column("name", "STRING", maxLength: 50),
        //    new Column("dept_id", "INT", constraint: new ColumnConstraint
        //    {
        //        Constraints = ConstraintType.ForeignKey,
        //        ReferenceTable = "Departments",
        //        ReferenceColumn = "id"
        //    })
        //};
        //        db.CreateTable("Students", studentColumns);
        //        var qp = new QueryProcessor(tm,dbms);
        //var result = qp.ExecuteJoinQuery("Students", "Departments", qp.GetQPDatabase());

        //foreach (var row in result)
        //{
        //    Console.WriteLine(string.Join(", ", row.Select(kv => $"{kv.Key}={kv.Value}")));
        //}

        //        db.Tables["Departments"].InsertRow(new Dictionary<string, string>
        //{
        //    { "id", "101" },
        //    { "name", "Computer Science" }
        //});
        //        db.Tables["Students"].InsertRow(new Dictionary<string, string>
        //{
        //    { "id", "1" },
        //    { "name", "Alice" },
        //    { "dept_id", "101" } // ✅ Valid
        ////});
        //        db.Tables["Students"].InsertRow(new Dictionary<string, string>
        //{
        //    { "id", "2" },
        //    { "name", "Bob" },
        //    { "dept_id", "999" } // ❌ Will throw exception
        //});


        //Guid tx1 = tm.BeginTransaction();
        //var row1 = new Dictionary<string, string>
        //{
        //  { "id", "4" },
        //  { "name", "Bob" },
        //  { "isStudent", "false" }
        //};
        //tm.LogOperation(tx1, "insert", "Students", null, new List<Dictionary<string, string>> { row1 }, db);
        //db.Tables["Students"].InsertRow(row1);

        //var row2 = new Dictionary<string, string>
        //{
        //  { "id", "5" },
        //  { "name", "Clarke" },
        //  { "isStudent", "false" }
        //};
        //Guid tx2 = tm.BeginTransaction();
        //tm.LogOperation(tx2, "insert", "Students", null, new List<Dictionary<string, string>> { row2 }, db);
        //db.Tables["Students"].InsertRow(row2);
        // tm.RollbackTransaction(tx1,db);
        //var visible1 = tm.GetVisibleRows(tx2, "Students", db);
        //foreach (var row in visible1)
        //    Console.WriteLine(string.Join(", ", row.Select(kv => $"{kv.Key}={kv.Value}")));
        //Console.WriteLine("1");
        //var visible = tm.GetVisibleRows(tx1, "Students", db);
        //foreach (var row in visible)
        //    Console.WriteLine(string.Join(", ", row.Select(kv => $"{kv.Key}={kv.Value}")));
        StorageManager.SaveDBMS(dBMS);
        //var db = new MVCCDatabase();
        //db.CreateTable("users");

        //var cc = new ConcurrencyControl();
        //var tm = new TransactionManager(db,cc);

        //// TX1 inserts
        //var tx1 = tm.BeginTransaction();
        //tm.Insert(tx1, "users", new Dictionary<string, string> { ["id"] = "1", ["name"] = "Alice" });
        //var data1 = tm.Read(tx1, "users");
        //Console.WriteLine($"TX1 sees {data1.Count} rows.");
        //// TX2 tries to read - should not see Alice
        //var tx2 = tm.BeginTransaction();
        //var data2 = tm.Read(tx2, "users");
        //tm.Insert(tx2, "users", new Dictionary<string, string> { ["id"] = "2", ["name"] = "Bob" });
        //Console.WriteLine($"TX2 sees {data2.Count} rows."); // 0 rows

        //// TX1 commits
        //tm.Commit(tx1);
        //// TX2 reads again - now sees 1 as the transaction is commited tx1

        //var stillInvisible = tm.Read(tx2, "users");
        //Console.WriteLine($"TX2 still sees {stillInvisible.Count} rows.");
        // tm.Insert(tx2, "users", new Dictionary<string, string> { ["id"] = "2", ["name"] = "Bob" });
        // var data3= tm.Read(tx1 , "users");
        // var data5 = tm.Read(tx2, "users");
        // Console.WriteLine($"TX1 sees {data3.Count} rows.");// will see 0 as transaction tx1 is being commited.
        //Console.WriteLine($"TX2 sees {data5.Count} rows.");
        //tm.Rollback(tx2);
        //// New transaction TX3 sees it
        //var tx3 = tm.BeginTransaction();
        //var data4 = tm.Read(tx3, "users");
        //Console.WriteLine($"TX3 sees {data4.Count} rows."); // 1 row



    }
}
