using System;
using System.Collections.Generic;
using System.Linq;

class QueryProcessor
{
    private Database database;
    private TransactionManager transactionManager;

    public QueryProcessor(Database db, TransactionManager tm)
    {
        database = db;
        transactionManager = tm;
    }

    public void ExecuteQuery(string query)
    {
        string[] parts = query.Split(" ");
        string command = parts[0].ToLower();

        switch (command)
        {
            case "create":
                HandleCreateQuery(parts);
                break;

            case "insert":
                HandleInsertQuery(parts);
                break;

            case "select":
                HandleSelectQuery(parts);
                break;

            case "update":
                HandleUpdateQuery(parts);
                break;

            case "delete":
                HandleDeleteQuery(parts);
                break;

            case "begin":
                transactionManager.BeginTransaction();
                break;

            case "commit":
                transactionManager.CommitTransaction();
                break;

            case "rollback":
                transactionManager.RollbackTransaction(database);
                break;

            default:
                Console.WriteLine("Invalid query.");
                break;
        }
    }

    private void HandleCreateQuery(string[] parts)
    {
        if (parts.Length < 4 || parts[1].ToLower() != "table")
        {
            Console.WriteLine("Syntax Error: Use CREATE TABLE <table_name> (column1, column2, ...)");
            return;
        }

        string tableName = parts[2];
        List<string> columns = parts.Skip(3).Select(col => col.Trim('(', ')', ',')).ToList();
        database.CreateTable(tableName, columns);
        Console.WriteLine($"Table '{tableName}' created successfully.");
    }

    private void HandleInsertQuery(string[] parts)
    {
        if (parts.Length < 4 || parts[1].ToLower() != "into")
        {
            Console.WriteLine("Syntax Error: Use INSERT INTO <table_name> (column1, column2, ...) VALUES (value1, value2, ...)");
            return;
        }

        string tableName = parts[2];
        if (!database.Tables.ContainsKey(tableName))
        {
            Console.WriteLine($"Table '{tableName}' does not exist.");
            return;
        }

        List<string> columns = parts[3].Trim('(', ')').Split(',').Select(c => c.Trim()).ToList();
        List<string> values = parts[5].Trim('(', ')').Split(',').Select(v => v.Trim()).ToList();

        if (columns.Count != values.Count)
        {
            Console.WriteLine("Error: Column count does not match value count.");
            return;
        }

        Dictionary<string, string> row = columns.Zip(values, (col, val) => new { col, val })
                                               .ToDictionary(x => x.col, x => x.val);

        if (transactionManager.IsInTransaction())
        {
            transactionManager.LogOperation("insert",tableName,null, new List<Dictionary<string, string>> { row }
);
        }

        database.Tables[tableName].InsertRow(row);
        Console.WriteLine("Row inserted successfully.");
    }

    private void HandleSelectQuery(string[] parts)
    {
        if (parts.Length < 4 || parts[1] != "*" || parts[2].ToLower() != "from")
        {
            Console.WriteLine("Syntax Error: Use SELECT * FROM <table_name>");
            return;
        }

        string tableName = parts[3];
        if (database.Tables.ContainsKey(tableName))
        {
            database.Tables[tableName].DisplayTable();
        }
        else
        {
            Console.WriteLine($"Table '{tableName}' does not exist.");
        }
    }

    private void HandleUpdateQuery(string[] parts)
    {
        if (parts.Length < 6 || !parts.Contains("set") || !parts.Contains("where"))
        {
            Console.WriteLine("Syntax Error: Use UPDATE <table_name> SET column=value WHERE column=value");
            return;
        }

        string tableName = parts[1].ToLower();
        if (!database.Tables.ContainsKey(tableName))
        {
            Console.WriteLine($"Table '{tableName}' does not exist.");
            return;
        }

        var table = database.Tables[tableName];

        int setIndex = Array.FindIndex(parts, p => p.ToLower() == "set");
        int whereIndex = Array.FindIndex(parts, p => p.ToLower() == "where");

        string[] setPair = parts[setIndex + 1].Split('=');
        if (setPair.Length != 2)
        {
            Console.WriteLine("Invalid SET clause.");
            return;
        }

        string setCol = setPair[0].Trim();
        string setVal = setPair[1].Trim();

        string[] wherePair = parts[whereIndex + 1].Split('=');
        if (wherePair.Length != 2)
        {
            Console.WriteLine("Invalid WHERE clause.");
            return;
        }

        string whereCol = wherePair[0].Trim();
        string whereVal = wherePair[1].Trim();

        var matchedRows = table.Rows.Where(r => r.ContainsKey(whereCol) && r[whereCol] == whereVal).ToList();
        if (matchedRows.Count == 0)
        {
            Console.WriteLine("No matching rows found to update.");
            return;
        }

        if (transactionManager.IsInTransaction())
        {
            var beforeRows = matchedRows.Select(r => new Dictionary<string, string>(r)).ToList();
            var afterRows = matchedRows.Select(r => { var copy = new Dictionary<string, string>(r); copy[setCol] = setVal; return copy; }).ToList();
            transactionManager.LogOperation("update", tableName, beforeRows, afterRows);
        }

        foreach (var row in matchedRows)
        {
            row[setCol] = setVal;
        }

        Console.WriteLine($"{matchedRows.Count} row(s) updated successfully in '{tableName}'.");
    }


    private void HandleDeleteQuery(string[] parts)
    {
        if (parts.Length < 4 || parts[1].ToLower() != "from")
        {
            Console.WriteLine("Syntax Error: Use DELETE FROM <table_name> WHERE column=value");
            return;
        }

        string tableName = parts[2];
        if (!database.Tables.ContainsKey(tableName))
        {
            Console.WriteLine($"Table '{tableName}' does not exist.");
            return;
        }

        string[] whereClause = parts[4].Split('=');
        string whereColumn = whereClause[0].Trim();
        string whereValue = whereClause[1].Trim();

        var rowsToDelete = database.Tables[tableName].Rows
            .Where(r => r.ContainsKey(whereColumn) && r[whereColumn] == whereValue)
            .ToList();

        if (rowsToDelete.Count == 0)
        {
            Console.WriteLine("No matching rows found to delete.");
            return;
        }

        if (transactionManager.IsInTransaction())
        {
            var beforeRows = rowsToDelete.Select(r => new Dictionary<string, string>(r)).ToList();
            transactionManager.LogOperation("delete", tableName, beforeRows, null);
        }

        database.Tables[tableName].Rows.RemoveAll(r => r.ContainsKey(whereColumn) && r[whereColumn] == whereValue);
        Console.WriteLine($"{rowsToDelete.Count} row(s) deleted successfully from '{tableName}'.");
    }

}
