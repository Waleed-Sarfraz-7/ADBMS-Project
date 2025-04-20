using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ConsoleApp1;

class QueryProcessor
{
    private Database database;
    private TransactionManager transactionManager;
    private readonly DBMS dbms;
    private Guid? currentTransactionId = null; // You should manage this at the class level

    public QueryProcessor( TransactionManager tm, DBMS dBMS)
    {
        transactionManager = tm;
        dbms = dBMS;
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
                HandleSelectQuery(query);
                break;

            case "update":
                HandleUpdateQuery(parts);
                break;

            case "delete":
                HandleDeleteQuery(parts);
                break;

            case "begin":
                HandleBeginTransaction();
                break;

            case "commit":
                HandleCommitTransaction();
                break;

            case "rollback":
                HandleRollbackTransaction();
                break;
            case "use":
                HandleUseDatabase(parts);
                break;
            

            default:
                Console.WriteLine("Invalid query.");
                break;
        }
    }
    public void HandleBeginTransaction()
    {
        currentTransactionId = transactionManager.BeginTransaction();
    }

    public void HandleCommitTransaction()
    {
        if (currentTransactionId.HasValue)
        {
            transactionManager.CommitTransaction(currentTransactionId.Value);
            currentTransactionId = null;
        }
    }
    public void HandleRollbackTransaction()
    {
        if (currentTransactionId.HasValue)
        {
            transactionManager.RollbackTransaction(currentTransactionId.Value, database);
            currentTransactionId = null;
        }
    }
    public List<Dictionary<string, string>> ExecuteJoinQuery(
    string leftTableName,
    string rightTableName,
    string leftJoinColumn,
    string rightJoinColumn,
    string joinType,
    Database db)
    {
        var leftTable = db.Tables[leftTableName];
        var rightTable = db.Tables[rightTableName];

        var result = new List<Dictionary<string, string>>();

        foreach (var leftRow in leftTable.Rows)
        {
            bool matchFound = false;
            Console.WriteLine("Left Row Keys: " + string.Join(", ", leftRow.Keys));


            foreach (var rightRow in rightTable.Rows)
            {
                if (leftRow.TryGetValue(leftJoinColumn, out string leftValue) &&
                    rightRow.TryGetValue(rightJoinColumn, out string rightValue) &&
                    leftValue == rightValue)
                {
                    var combined = new Dictionary<string, string>();

                    foreach (var kv in leftRow)
                        combined[$"{leftTableName}.{kv.Key}"] = kv.Value;

                    foreach (var kv in rightRow)
                        combined[$"{rightTableName}.{kv.Key}"] = kv.Value;

                    result.Add(combined);
                    matchFound = true;
                }
            }

            
        


        if ((joinType == "LEFT" || joinType == "FULL") && !matchFound)
            {
                var combined = new Dictionary<string, string>();
                foreach (var kv in leftRow)
                    combined[$"{leftTableName}.{kv.Key}"] = kv.Value;

                foreach (var col in rightTable.Columns)
                    combined[$"{rightTableName}.{col.Name}"] = "NULL";

                result.Add(combined);
            }
        }

        if (joinType == "RIGHT" || joinType == "FULL")
        {
            foreach (var rightRow in rightTable.Rows)
            {
                bool matchFound = result.Any(r =>
                    r.ContainsKey($"{rightTableName}.{rightJoinColumn}") &&
                    r[$"{rightTableName}.{rightJoinColumn}"] == rightRow[rightJoinColumn]);

                if (!matchFound)
                {
                    var combined = new Dictionary<string, string>();
                    foreach (var col in leftTable.Columns)
                        combined[$"{leftTableName}.{col.Name}"] = "NULL";

                    foreach (var kv in rightRow)
                        combined[$"{rightTableName}.{kv.Key}"] = kv.Value;

                    result.Add(combined);
                }
            }
        }

        return result;
    }


    private void HandleUseDatabase(string[] parts)
    {
        if (parts.Length != 2)
        {
            Console.WriteLine("Usage: USE <database_name>");
            return;
        }

        string dbName = parts[1];
        if (dbms.UseDatabase(dbName))
        {
           
            database = dbms.GetCurrentDatabase();
            Console.WriteLine($"Switched to database '{dbName}'.");
        }
            
        else
            Console.WriteLine($"Database '{dbName}' not found.");
    }

    private void HandleCreateQuery(string[] parts)
    {
        if (parts.Length < 3)
        {
            Console.WriteLine("Syntax Error: Incomplete CREATE command.");
            return;
        }

        string objectType = parts[1].ToLower();

        if (objectType == "database")
        {
            if (parts.Length < 3)
            {
                Console.WriteLine("Syntax Error: Use CREATE DATABASE <database_name>");
                return;
            }

            string dbName = parts[2].TrimEnd(';');
            dbms.CreateDatabase(dbName); // Assuming 'dbms' is your DBMS instance
            Console.WriteLine($"Database '{dbName}' created successfully.");
        }
        else if (objectType == "table")
        {
            if (parts.Length < 4)
            {
                Console.WriteLine("Syntax Error: Use CREATE TABLE <table_name> (column_name TYPE [LENGTH] [CONSTRAINTS], ...)");
                return;
            }

            string tableName = parts[2];
            string columnPart = string.Join(" ", parts.Skip(3));
            int start = columnPart.IndexOf('(');
            int end = columnPart.IndexOf(')');

            if (start == -1 || end == -1 || end <= start)
            {
                Console.WriteLine("Syntax Error: Columns must be in parentheses.");
                return;
            }

            string columnString = columnPart.Substring(start + 1, end - start - 1);
            string[] columnDefs = columnString.Split(',');

            List<Column> columns = new List<Column>();

            foreach (string rawCol in columnDefs)
            {
                string[] tokens = rawCol.Trim().Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

                if (tokens.Length < 2)
                {
                    Console.WriteLine($"Syntax Error: Invalid column definition '{rawCol.Trim()}'");
                    return;
                }

                string colName = tokens[0];
                string dataType = tokens[1].ToUpper();
                int? maxLength = null;
                int index = 2;

                if (dataType == "STRING" && tokens.Length > index && int.TryParse(tokens[index], out int len))
                {
                    maxLength = len;
                    index++;
                }

                ColumnConstraint constraint = new ColumnConstraint();

                while (index < tokens.Length)
                {
                    string token = tokens[index].ToUpper();

                    switch (token)
                    {
                        case "NOT_NULL":
                        case "NOTNULL":
                        case "NOT":
                            constraint.Constraints |= ConstraintType.NotNull;
                            break;

                        case "UNIQUE":
                            constraint.Constraints |= ConstraintType.Unique;
                            break;

                        case "PRIMARY_KEY":
                        case "PRIMARYKEY":
                        case "PRIMARY":
                            constraint.Constraints |= ConstraintType.PrimaryKey;
                            break;

                        case "DEFAULT":
                            index++;
                            if (index < tokens.Length)
                                constraint.DefaultValue = tokens[index];
                            else
                            {
                                Console.WriteLine("Syntax Error: DEFAULT value missing.");
                                return;
                            }
                            break;

                        case "CHECK":
                            constraint.Constraints |= ConstraintType.Check;
                            index++;
                            if (index < tokens.Length && tokens[index].StartsWith("("))
                            {
                                StringBuilder checkExpr = new StringBuilder(tokens[index]);
                                index++;
                                while (index < tokens.Length && !tokens[index].EndsWith(")"))
                                {
                                    checkExpr.Append(" ").Append(tokens[index]);
                                    index++;
                                }

                                if (index < tokens.Length)
                                {
                                    checkExpr.Append(" ").Append(tokens[index]); // Append last part
                                    constraint.CheckExpression = checkExpr.ToString().Trim('(', ')');
                                }
                                else
                                {
                                    Console.WriteLine("Syntax Error: Invalid CHECK expression.");
                                    return;
                                }
                            }
                            else
                            {
                                Console.WriteLine("Syntax Error: CHECK expression missing.");
                                return;
                            }
                            break;

                        case "FOREIGN_KEY":
                        case "FOREIGNKEY":
                            constraint.Constraints |= ConstraintType.ForeignKey;
                            index++;
                            if (index < tokens.Length && tokens[index].Contains("(") && tokens[index].Contains(")"))
                            {
                                string refDef = tokens[index];
                                int parenStart = refDef.IndexOf('(');
                                int parenEnd = refDef.IndexOf(')');
                                constraint.ReferenceTable = refDef.Substring(0, parenStart);
                                constraint.ReferenceColumn = refDef.Substring(parenStart + 1, parenEnd - parenStart - 1);
                            }
                            else
                            {
                                Console.WriteLine("Syntax Error: FOREIGN_KEY format should be <table(column)>.");
                                return;
                            }
                            break;

                        default:
                            Console.WriteLine($"Warning: Unknown token '{token}' ignored.");
                            break;
                    }

                    index++;
                }

                columns.Add(new Column(colName, dataType, maxLength, constraint));
            }

            if (database == null)
            {
                Console.WriteLine("No database selected. Use USE <database_name> before creating tables.");
                return;
            }

            database.CreateTable(tableName, columns);
            Console.WriteLine($"Table '{tableName}' created successfully in database '{database.Name}'.");
        }

    }
    public Database GetQPDatabase()
    {
        return database;
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

        if (currentTransactionId.HasValue)
        {
            // Log the operation before performing it
            transactionManager.LogOperation(
                currentTransactionId.Value,
                "insert",
                tableName,
                null,                           // No "before" state for insert
                new List<Dictionary<string, string>> { row },
                database
            );
        }

        database.Tables[tableName].InsertRow(row);
        Console.WriteLine("Row inserted successfully.");
    }

    public void HandleSelectQuery(string query)
    {
        query = query.Trim();
        var originalQuery = query;
        query = query.Replace(",", " , "); // Ensures columns split correctly
        var parts = query.Split(' ', StringSplitOptions.RemoveEmptyEntries);

        if (parts.Length < 4 || parts[0].ToUpper() != "SELECT")
        {
            Console.WriteLine("Syntax Error: Invalid SELECT query.");
            return;
        }

        // 1. Extract columns
        int fromIndex = Array.IndexOf(parts, "FROM");
        if (fromIndex == -1)
        {
            Console.WriteLine("Syntax Error: 'FROM' keyword missing.");
            return;
        }

        var selectedColumns = parts.Skip(1).Take(fromIndex - 1).ToList();
        bool selectAll = selectedColumns.Count == 1 && selectedColumns[0] == "*";

        // 2. Handle JOIN query
        if (parts.Contains("JOIN"))
        {
            string table1 = parts[fromIndex + 1];
            string joinType = parts[fromIndex + 2].ToUpper() == "JOIN" ? "INNER" : parts[fromIndex + 2].ToUpper(); // INNER, LEFT, RIGHT, FULL
            string table2 = joinType == "INNER" ? parts[fromIndex + 3] : parts[fromIndex + 4];

            int onIndex = Array.IndexOf(parts, "ON");
            if (onIndex == -1 || onIndex + 3 >= parts.Length)
            {
                Console.WriteLine("Syntax Error: JOIN condition missing.");
                return;
            }

            var leftJoinField = parts[onIndex + 1].Split('.');
            var rightJoinField = parts[onIndex + 3].Split('.');

            if (leftJoinField.Length != 2 || rightJoinField.Length != 2)
            {
                Console.WriteLine("Syntax Error: JOIN fields must be in format Table.Column");
                return;
            }

            string leftTable = leftJoinField[0];
            string leftColumn = leftJoinField[1];
            string rightTable = rightJoinField[0];
            string rightColumn = rightJoinField[1];

            var results = ExecuteJoinQuery(leftTable, rightTable, leftColumn, rightColumn, joinType, database);

            // Print results
            foreach (var row in results)
            {
                Console.WriteLine(string.Join(" | ", selectAll
                    ? row.Select(kv => $"{kv.Key}: {kv.Value}")
                    : row.Where(kv => selectedColumns.Contains(kv.Key)).Select(kv => $"{kv.Key}: {kv.Value}")));
            }

            return;
        }

        // 3. Handle Simple SELECT (no join)
        string tableName = parts[fromIndex + 1];
        if (!database.Tables.TryGetValue(tableName, out var table))
        {
            Console.WriteLine($"Table '{tableName}' does not exist.");
            return;
        }

        foreach (var row in table.Rows)
        {
            if (selectAll)
            {
                Console.WriteLine(string.Join(" | ", row.Select(kv => $"{kv.Key}: {kv.Value}")));
            }
            else
            {
                var colNames = table.Columns.Select(c => c.Name).ToList();
                foreach (var col in selectedColumns)
                {
                    if (!colNames.Contains(col))
                    {
                        Console.WriteLine($"Column '{col}' does not exist in table '{tableName}'.");
                        return;
                    }
                }

                Console.WriteLine(string.Join(" | ", selectedColumns.Select(col => $"{col}: {row[col]}")));
            }
        }
    }



    private void HandleUpdateQuery(string[] parts)
    {
        if (parts.Length < 6 || !parts.Contains("set") || !parts.Contains("where"))
        {
            Console.WriteLine("Syntax Error: Use UPDATE <table_name> SET column=value WHERE column=value");
            return;
        }

        string tableName = parts[1];
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

        if (currentTransactionId.HasValue)
        {
            var beforeRows = matchedRows.Select(r => new Dictionary<string, string>(r)).ToList();
            var afterRows = matchedRows.Select(r => { var copy = new Dictionary<string, string>(r); copy[setCol] = setVal; return copy; }).ToList();
            transactionManager.LogOperation(currentTransactionId.Value,"update", tableName, beforeRows, afterRows,database);
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

        if (currentTransactionId.HasValue)
        {
            var beforeRows = rowsToDelete.Select(r => new Dictionary<string, string>(r)).ToList();
            transactionManager.LogOperation(currentTransactionId.Value,"delete", tableName, beforeRows, null,database);
        }

        database.Tables[tableName].Rows.RemoveAll(r => r.ContainsKey(whereColumn) && r[whereColumn] == whereValue);
        Console.WriteLine($"{rowsToDelete.Count} row(s) deleted successfully from '{tableName}'.");
    }
    public void HandleReadTransaction(string[] parts)
    {
        string tableName = parts[1];
        if (!database.Tables.ContainsKey(tableName)) {
            Console.WriteLine("Table not exist");
        }

    }
}
