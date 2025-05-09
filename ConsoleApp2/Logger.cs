using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ConsoleApp1;

class AcidTest
{
    public static void Run()
    {
        Console.WriteLine("=== ACID Properties Test ===");

        var dbms = new DBMS();
        dbms.CreateDatabase("TestDB");
        dbms.UseDatabase("TestDB");

        var db = dbms.GetCurrentDatabase();
        var cc = new ConcurrencyControl();
        var tm = new TransactionManager(cc);

        db.CreateTable("Users", new List<Column> {
            new Column("Id", "STRING", constraint: new ColumnConstraint { Constraints = ConstraintType.PrimaryKey }),
            new Column("Name", "STRING", constraint: new ColumnConstraint { Constraints = ConstraintType.NotNull })
        });

        // Atomicity
        Console.WriteLine("\n[Test 1] Atomicity - Rollback on failure");
        var tx1 = tm.BeginTransaction();
        db.InsertRow(tx1, "Users", new Dictionary<string, string> { { "Id", "1" }, { "Name", "Alice" } }, tm);
        db.InsertRow(tx1, "Users", new Dictionary<string, string> { { "Id", "2" }, { "Name", "" } }, tm); // Should fail NotNull
        tm.RollbackTransaction(tx1, db);
        PrintTable(db, "Users");

        // Consistency
        Console.WriteLine("\n[Test 2] Consistency - Data valid after commit");
        var tx2 = tm.BeginTransaction();
        db.InsertRow(tx2, "Users", new Dictionary<string, string> { { "Id", "3" }, { "Name", "Bob" } }, tm);
        tm.CommitTransaction(tx2, db);
        PrintTable(db, "Users");

        // Isolation
        Console.WriteLine("\n[Test 3] Isolation - Concurrent Transactions");
        var tx3 = tm.BeginTransaction();
        var tx4 = tm.BeginTransaction();

        var t1 = Task.Run(() =>
        {
            db.InsertRow(tx3, "Users", new Dictionary<string, string> { { "Id", "4" }, { "Name", "Charlie" } }, tm);
            Thread.Sleep(100); // Simulate delay
            tm.CommitTransaction(tx3, db);
        });

        var t2 = Task.Run(() =>
        {
            Thread.Sleep(50); // Run while tx3 is uncommitted
            var rows = tm.GetVisibleRows(tx4, "Users", db);
            Console.WriteLine($"Tx4 sees {rows.Count} row(s)");
            tm.CommitTransaction(tx4, db);
        });

        Task.WaitAll(t1, t2);
        PrintTable(db, "Users");

        // Durability
        Console.WriteLine("\n[Test 4] Durability - Changes persist after commit");
        var tx5 = tm.BeginTransaction();
        db.InsertRow(tx5, "Users", new Dictionary<string, string> { { "Id", "5" }, { "Name", "Eve" } }, tm);
        tm.CommitTransaction(tx5, db);

        // Simulate restart (re-read DB)
        var tx6 = tm.BeginTransaction();
        var visible = tm.GetVisibleRows(tx6, "Users", db);
        Console.WriteLine($"After restart, Tx6 sees {visible.Count} row(s)");
        PrintTable(db, "Users");

        Console.WriteLine("\n=== ACID Tests Complete ===");
    }

    private static void PrintTable(Database db, string tableName)
    {
        Console.WriteLine($"--- {tableName} ---");
        if (db.Tables.TryGetValue(tableName, out var table))
        {
            foreach (var row in table.Rows)
                Console.WriteLine(string.Join(", ", row.Select(kv => $"{kv.Key}={kv.Value}")));
        }
        else
        {
            Console.WriteLine("Table not found.");
        }
    }
}
