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
            transactionManager.LogOperation(tableName, row);
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
        if (parts.Length < 6 || parts[1].ToLower() != "set")
        {
            Console.WriteLine("Syntax Error: Use UPDATE <table_name> SET column=value WHERE column=value");
            return;
        }

        string tableName = parts[0];
        if (!database.Tables.ContainsKey(tableName))
        {
            Console.WriteLine($"Table '{tableName}' does not exist.");
            return;
        }

        string[] setClause = parts[2].Split('=');
        string setColumn = setClause[0].Trim();
        string setValue = setClause[1].Trim();

        string[] whereClause = parts[4].Split('=');
        string whereColumn = whereClause[0].Trim();
        string whereValue = whereClause[1].Trim();

        if (transactionManager.IsInTransaction())
        {
            Dictionary<string, string> oldRow = database.Tables[tableName].Rows.FirstOrDefault(r => r[whereColumn] == whereValue);
            if (oldRow != null)
            {
                transactionManager.LogOperation(tableName, oldRow);
            }
        }

        database.Tables[tableName].Rows
            .Where(r => r[whereColumn] == whereValue)
            .ToList()
            .ForEach(r => r[setColumn] = setValue);

        Console.WriteLine("Update successful.");
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

        if (transactionManager.IsInTransaction())
        {
            List<Dictionary<string, string>> rowsToDelete = database.Tables[tableName].Rows
                .Where(r => r[whereColumn] == whereValue)
                .ToList();

            foreach (var row in rowsToDelete)
            {
                transactionManager.LogOperation(tableName, row);
            }
        }

        database.Tables[tableName].Rows.RemoveAll(r => r[whereColumn] == whereValue);
        Console.WriteLine("Row(s) deleted successfully.");
    }
}
