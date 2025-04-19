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
               // transactionManager.CommitTransaction();
                break;

            case "rollback":
               // transactionManager.RollbackTransaction(database);
                break;
            case "use":
                HandleUseDatabase(parts);
                break;
            

            default:
                Console.WriteLine("Invalid query.");
                break;
        }
    }
    public List<Dictionary<string, string>> ExecuteJoinQuery(
        string table1Name,
        string table2Name,
        string table1JoinColumn,
        string table2JoinColumn,
        Database db)
    {
        if (!db.Tables.ContainsKey(table1Name) || !db.Tables.ContainsKey(table2Name))
        {
            Console.WriteLine("One or both tables not found.");
            return new List<Dictionary<string, string>>();
        }

        var table1 = db.Tables[table1Name];
        var table2 = db.Tables[table2Name];

        var result = new List<Dictionary<string, string>>();

        foreach (var row1 in table1.Rows)
        {
            if (!row1.ContainsKey(table1JoinColumn)) continue;

            string value1 = row1[table1JoinColumn];

            foreach (var row2 in table2.Rows)
            {
                if (!row2.ContainsKey(table2JoinColumn)) continue;

                string value2 = row2[table2JoinColumn];

                if (value1 == value2)
                {
                    var combined = new Dictionary<string, string>();

                    foreach (var kv in row1)
                        combined[$"{table1Name}.{kv.Key}"] = kv.Value;

                    foreach (var kv in row2)
                        combined[$"{table2Name}.{kv.Key}"] = kv.Value;

                    result.Add(combined);
                }
            }
        }

        return result;
    }
    public List<Dictionary<string, string>> ExecuteJoinQuery(string leftTableName, string rightTableName, Database db)
    {
        var leftTable = db.Tables[leftTableName];
        var rightTable = db.Tables[rightTableName];

        var leftJoinColumn = leftTable.Columns
            .FirstOrDefault(c => c.Constraint.Has(ConstraintType.ForeignKey) &&
                                 c.Constraint.ReferenceTable == rightTableName);

        if (leftJoinColumn == null)
        {
            Console.WriteLine("No foreign key relationship found.");
            return new List<Dictionary<string, string>>();
        }

        string rightJoinColumn = leftJoinColumn.Constraint.ReferenceColumn;
        return ExecuteJoinQuery(leftTableName, rightTableName, leftJoinColumn.Name, rightJoinColumn, db);
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

        // if (transactionManager.IsInTransaction())
        // {
        //     transactionManager.LogOperation("insert",tableName,null, new List<Dictionary<string, string>> { row }
        //
        // }

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

       //if (transactionManager.IsInTransaction())
       //{
       //    var beforeRows = matchedRows.Select(r => new Dictionary<string, string>(r)).ToList();
       //    var afterRows = matchedRows.Select(r => { var copy = new Dictionary<string, string>(r); copy[setCol] = setVal; return copy; }).ToList();
       //    transactionManager.LogOperation("update", tableName, beforeRows, afterRows);
       //}

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

      //  if (transactionManager.IsInTransaction())
      //  {
      //      var beforeRows = rowsToDelete.Select(r => new Dictionary<string, string>(r)).ToList();
      //      transactionManager.LogOperation("delete", tableName, beforeRows, null);
      //  }

        database.Tables[tableName].Rows.RemoveAll(r => r.ContainsKey(whereColumn) && r[whereColumn] == whereValue);
        Console.WriteLine($"{rowsToDelete.Count} row(s) deleted successfully from '{tableName}'.");
    }

}
