﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using ConsoleApp1;

class QueryProcessor
{
    private Database database;
    private TransactionManager transactionManager;
    private readonly DBMS dbms;
    private static ThreadLocal<Guid?> threadTransactionId = new(() => null);


    public QueryProcessor(TransactionManager tm, DBMS dBMS)
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
            case "update":
            case "delete":
            case "select":
                if (threadTransactionId.Value.HasValue)
                    ExecuteInTransaction(query, threadTransactionId.Value.Value);
                else
                    ExecuteQueryWithoutTransaction(query);
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
            case "explain":
                ExplainQuery(query);
                break;
            default:
                Console.WriteLine("Invalid query.");
                break;
        }

    }
    private void ExecuteQueryWithoutTransaction(string query)
    {
        string[] parts = query.Split(" ");
        string command = parts[0].ToLower();

        switch (command)
        {
            case "insert":
                HandleInsertQuery(parts);
                break;
            case "update":
                HandleUpdateQuery(parts);
                break;
            case "delete":
                HandleDeleteQuery(parts);
                break;
            case "select":
                HandleSelectQuery(query);
                break;
            
            default:
                Console.WriteLine("Unsupported query outside transaction.");
                break;
        }
    }

    public void SetDatabase(string dbName)
    {
        if (dbms.Databases.ContainsKey(dbName))
        {
            database = dbms.Databases[dbName];
            Console.WriteLine($"Using database: {dbName}");
        }
        else
        {
            Console.WriteLine($"Database '{dbName}' does not exist.");
        }
    }


    public DBMS GetDBMS()
    {
        return dbms;
    }
    /// <summary>
    /// Launches a new interactive transaction window.
    /// </summary>
    /// 
    private void HandleBeginTransaction()
    {
        if (threadTransactionId.Value.HasValue)
        {
            Console.WriteLine("A transaction is already active in this thread.");
            return;
        }

        var txId = transactionManager.BeginTransaction();
        threadTransactionId.Value = txId;
        Console.WriteLine($"Transaction started (ID: {txId})");
    }

    private void HandleCommitTransaction()
    {
        if (!threadTransactionId.Value.HasValue)
        {
            Console.WriteLine("No active transaction to commit.");
            return;
        }

        transactionManager.CommitTransaction(threadTransactionId.Value.Value, database);
        Console.WriteLine("Transaction committed.");
        threadTransactionId.Value = null;
    }

    private void HandleRollbackTransaction()
    {
        if (!threadTransactionId.Value.HasValue)
        {
            Console.WriteLine("No active transaction to rollback.");
            return;
        }

        transactionManager.RollbackTransaction(threadTransactionId.Value.Value, database);
        Console.WriteLine("Transaction rolled back.");
        threadTransactionId.Value = null;
    }


    private void ExecuteInTransaction(string query, Guid transactionId)
    {
        string[] parts = query.Split(" ");
        string command = parts[0].ToLower();

        switch (command)
        {
            case "insert":
                HandleInsertQuery(parts, transactionId);
                break;
            case "update":
                HandleUpdateQuery(parts, transactionId);
                break;
            case "delete":
                HandleDeleteQuery(parts, transactionId);
                break;
            case "select":
                HandleSelectQuery(query, transactionId);
                break;
            case "use":
                HandleUseDatabase(parts);
                break;
            default:
                Console.WriteLine("Only DML/SELECT allowed inside a transaction.");
                break;
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
    public List<Dictionary<string, string>> ExecuteJoinQuery(
    string leftTableName,
    string rightTableName,
    string leftJoinColumn,
    string rightJoinColumn,
    string joinType,
    Guid transactionId,
    Database db)
    {
        var leftTable = db.Tables[leftTableName];
        var rightTable = db.Tables[rightTableName];

        var leftRows = transactionManager.GetVisibleRows(transactionId, leftTableName, db);
        var rightRows = transactionManager.GetVisibleRows(transactionId, rightTableName, db);

        var result = new List<Dictionary<string, string>>();

        foreach (var leftRow in leftRows)
        {
            bool matchFound = false;
            foreach (var rightRow in rightRows)
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
            foreach (var rightRow in rightRows)
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
            int end = columnPart.LastIndexOf(')');

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
                string[] tokens = Regex.Matches(rawCol.Trim(), @"[^\s""]+|""[^""]*""")
                                       .Cast<Match>()
                                       .Select(m => m.Value)
                                       .ToArray();

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
                            StringBuilder checkExpr = new StringBuilder();

                            while (index < tokens.Length)
                            {
                                checkExpr.Append(tokens[index]).Append(" ");
                                if (tokens[index].EndsWith(")"))
                                    break;
                                index++;
                            }

                            if (checkExpr.Length == 0 || !checkExpr.ToString().Contains("("))
                            {
                                Console.WriteLine("Syntax Error: Invalid CHECK expression.");
                                return;
                            }

                            constraint.CheckExpression = checkExpr.ToString().Trim().Trim('(', ')');
                            break;

                        case "FOREIGN_KEY":
                        case "FOREIGNKEY":
                            constraint.Constraints |= ConstraintType.ForeignKey;
                            index++;

                            if (index < tokens.Length)
                            {
                                string fkDef = string.Join(" ", tokens.Skip(index));
                                var match = Regex.Match(fkDef, @"^([A-Za-z_][\w]*)\s*\(\s*([A-Za-z_][\w]*)\s*\)");

                                if (match.Success)
                                {
                                    constraint.ReferenceTable = match.Groups[1].Value;
                                    constraint.ReferenceColumn = match.Groups[2].Value;
                                    index = tokens.Length; // All consumed
                                }
                                else
                                {
                                    Console.WriteLine("Syntax Error: FOREIGN_KEY format should be <table(column)>.");
                                    return;
                                }
                            }
                            else
                            {
                                Console.WriteLine("Syntax Error: Missing FOREIGN_KEY reference.");
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

        else if (objectType == "index")
        {
            if (parts.Length < 5)
            {
                Console.WriteLine("Syntax Error: Use CREATE INDEX <index_name> ON <table_name>(<column_name>)");
                return;
            }

            string indexName = parts[2];
            string onKeyword = parts[3].ToLower();

            if (onKeyword != "on")
            {
                Console.WriteLine("Syntax Error: Missing 'ON' keyword in CREATE INDEX statement.");
                return;
            }

            string tableAndColumnPart = string.Join(" ", parts.Skip(4));
            int startParen = tableAndColumnPart.IndexOf('(');
            int endParen = tableAndColumnPart.IndexOf(')');

            if (startParen == -1 || endParen == -1 || endParen <= startParen)
            {
                Console.WriteLine("Syntax Error: Column name must be inside parentheses.");
                return;
            }

            string tableName = tableAndColumnPart.Substring(0, startParen).Trim();
            string columnName = tableAndColumnPart.Substring(startParen + 1, endParen - startParen - 1).Trim();

            if (database == null)
            {
                Console.WriteLine("No database selected. Use USE <database_name> before creating indexes.");
                return;
            }

            Table table = database.GetTable(tableName);

            if (table == null)
            {
                Console.WriteLine($"Table '{tableName}' does not exist in database '{database.Name}'.");
                return;
            }

            if (!table.HasColumn(columnName))
            {
                Console.WriteLine($"Column '{columnName}' does not exist in table '{tableName}'.");
                return;
            }

            // ✅ Create the index (StorageManager will handle saving automatically)
            table.CreateIndex(columnName);

            Console.WriteLine($"Index '{indexName}' created successfully on '{tableName}({columnName})'.");

            
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

        // Properly parse columns and values
        string columnsPart = parts[3];
        string valuesPart = string.Join(" ", parts.Skip(5)); // In case values have spaces

        List<string> columns = columnsPart.Trim('(', ')')
                                           .Split(',')
                                           .Select(c => c.Trim())
                                           .ToList();

        List<string> values = ParseValues(valuesPart.Trim('(', ')'));

        if (columns.Count != values.Count)
        {
            Console.WriteLine("Error: Column count does not match value count.");
            return;
        }

        Dictionary<string, string> row = columns.Zip(values, (col, val) => new { col, val })
                                               .ToDictionary(x => x.col, x => x.val);

        

        database.Tables[tableName].InsertRow(row);
        Console.WriteLine("Row inserted successfully.");
    }
    private void HandleInsertQuery(string[] parts, Guid id)
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

        string columnsPart = parts[3];
        string valuesPart = string.Join(" ", parts.Skip(5)); // In case values have spaces

        List<string> columns = columnsPart.Trim('(', ')')
                                           .Split(',')
                                           .Select(c => c.Trim())
                                           .ToList();

        List<string> values = ParseValues(valuesPart.Trim('(', ')'));

        if (columns.Count != values.Count)
        {
            Console.WriteLine("Error: Column count does not match value count.");
            return;
        }

        Dictionary<string, string> row = columns.Zip(values, (col, val) => new { col, val })
                                               .ToDictionary(x => x.col, x => x.val);

        transactionManager.LogOperation(
            id,
            "insert",
            tableName,
            null,
            new List<Dictionary<string, string>> { row },
            database
        );

        //database.Tables[tableName].InsertRow(row);
        Console.WriteLine("Row inserted successfully.");
    }


    // Helper to parse values respecting quotes
    private List<string> ParseValues(string input)
    {
        List<string> values = new List<string>();
        bool insideQuotes = false;
        string current = "";

        foreach (char c in input)
        {
            if (c == '"')
            {
                insideQuotes = !insideQuotes;
                continue;
            }

            if (c == ',' && !insideQuotes)
            {
                values.Add(current.Trim());
                current = "";
            }
            else
            {
                current += c;
            }
        }

        if (!string.IsNullOrEmpty(current))
        {
            values.Add(current.Trim());
        }

        return values;
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

        // 1. Extract WHERE, GROUP BY, HAVING, and ORDER BY clauses if present
        string whereClause = null;
        string groupByClause = null;
        string havingClause = null;
        string orderByColumn = null;
        bool orderByDescending = false;

        int whereIndex = Array.FindIndex(parts, p => p.Equals("WHERE", StringComparison.OrdinalIgnoreCase));
        int groupByIndex = Array.FindIndex(parts, p => p.Equals("GROUP", StringComparison.OrdinalIgnoreCase));
        int havingIndex = Array.FindIndex(parts, p => p.Equals("HAVING", StringComparison.OrdinalIgnoreCase));
        int orderIndex = Array.FindIndex(parts, p => p.Equals("ORDER", StringComparison.OrdinalIgnoreCase));

        if (orderIndex != -1 && orderIndex + 2 < parts.Length && parts[orderIndex + 1].Equals("BY", StringComparison.OrdinalIgnoreCase))
        {
            orderByColumn = parts[orderIndex + 2];
            orderByDescending = orderIndex + 3 < parts.Length && parts[orderIndex + 3].Equals("DESC", StringComparison.OrdinalIgnoreCase);
        }

        if (groupByIndex != -1 && groupByIndex + 1 < parts.Length && parts[groupByIndex + 1].Equals("BY", StringComparison.OrdinalIgnoreCase))
        {
            int start = groupByIndex + 2;
            int end = (havingIndex != -1) ? havingIndex : (orderIndex != -1 ? orderIndex : parts.Length);
            groupByClause = string.Join(" ", parts.Skip(start).Take(end - start));
        }

        if (havingIndex != -1)
        {
            int start = havingIndex + 1;
            int end = (orderIndex != -1) ? orderIndex : parts.Length;
            havingClause = string.Join(" ", parts.Skip(start).Take(end - start));
        }

        if (whereIndex != -1)
        {
            int start = whereIndex + 1;
            int end = (groupByIndex == -1) ? (orderIndex == -1 ? parts.Length : orderIndex) : groupByIndex;
            whereClause = string.Join(" ", parts.Skip(start).Take(end - start));
        }

        // 2. Extract columns and table name
        int fromIndex = Array.IndexOf(parts, "FROM");
        if (fromIndex == -1)
        {
            Console.WriteLine("Syntax Error: 'FROM' keyword missing.");
            return;
        }

        var selectedColumns = parts.Skip(1).Take(fromIndex - 1).Where(p => p != ",").ToList();
        bool selectAll = selectedColumns.Count == 1 && selectedColumns[0] == "*";

        // 3. Handle JOIN query
        if (parts.Contains("JOIN"))
        {
            string table1 = parts[fromIndex + 1];
            string joinType = parts[fromIndex + 2].ToUpper() == "JOIN" ? "INNER" : parts[fromIndex + 2].ToUpper();
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

            // Apply WHERE clause
            if (!string.IsNullOrEmpty(whereClause))
            {
                results = results.Where(row => EvaluateWhereClauseWithQuotes(row, whereClause)).ToList();
            }

            // Apply GROUP BY
            if (!string.IsNullOrEmpty(groupByClause))
            {
                results = ApplyGroupBy(results, groupByClause);
            }

            // Apply HAVING clause
            if (!string.IsNullOrEmpty(havingClause))
            {
                results = results.Where(row => EvaluateWhereClause(row, havingClause)).ToList();
            }

            // Apply ORDER BY
            if (!string.IsNullOrEmpty(orderByColumn))
            {
                results = ApplyOrderBy(results, orderByColumn, orderByDescending);
            }

            // Print results
            foreach (var row in results)
            {
                Console.WriteLine(string.Join(" | ", selectAll
                    ? row.Select(kv => $"{kv.Key}: {kv.Value}")
                    : row.Where(kv => selectedColumns.Contains(kv.Key)).Select(kv => $"{kv.Key}: {kv.Value}")));
            }

            return;
        }

        // 4. Handle Simple SELECT (no JOIN)
        string tableName = parts[fromIndex + 1];
        if (!database.Tables.TryGetValue(tableName, out var table))
        {
            Console.WriteLine($"Table '{tableName}' does not exist.");
            return;
        }

        var rows = table.Rows;

        
        // Apply WHERE clause with optional index usage
        if (!string.IsNullOrEmpty(whereClause))
        {
            // Normalize WHERE clause to ensure spaces around '='
            string normalizedWhere = Regex.Replace(whereClause, @"([^\s])=([^\s])", "$1 = $2");
            var whereParts = normalizedWhere.Split(' ', StringSplitOptions.RemoveEmptyEntries);

            if (whereParts.Length == 3 && whereParts[1] == "=")
            {
                string column = whereParts[0];
                string value = whereParts[2].Trim('\'', '"');



                if (table.Indexes.TryGetValue(column, out var index))
                    {
                         
                        Console.WriteLine($"✅ Using index on column '{column}' for WHERE clause.");
                        rows = index.LookupRows(value);
                    }

                else
                {
                    Console.WriteLine($"❌ No index on column '{column}'; using full scan.");
                    rows = rows.Where(row => EvaluateWhereClause(row, whereClause)).ToList();
                }
            }
            else
            {
                Console.WriteLine("❌ Complex WHERE clause; using full scan.");
                rows = rows.Where(row => EvaluateWhereClause(row, whereClause)).ToList();
            }

        }


        // Apply GROUP BY
        if (!string.IsNullOrEmpty(groupByClause))
        {
            rows = ApplyGroupBy(rows, groupByClause);
        }

        // Apply HAVING clause
        if (!string.IsNullOrEmpty(havingClause))
        {
            rows = rows.Where(row => EvaluateWhereClause(row, havingClause)).ToList();
        }

        // Apply ORDER BY
        if (!string.IsNullOrEmpty(orderByColumn))
        {
            rows = ApplyOrderBy(rows, orderByColumn, orderByDescending);
        }

        // Print results
        foreach (var row in rows)
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
    public void ExplainQuery(string query)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        query = query.Trim();
        if (!query.StartsWith("EXPLAIN", StringComparison.OrdinalIgnoreCase))
        {
            Console.WriteLine("Syntax Error: Query must start with 'EXPLAIN'.");
            return;
        }

        // Strip EXPLAIN keyword
        string innerQuery = query.Substring(7).Trim();

        // Only support basic SELECT with WHERE = clause for now
        var parts = innerQuery.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 4 || !parts[0].Equals("SELECT", StringComparison.OrdinalIgnoreCase))
        {
            Console.WriteLine("EXPLAIN only supports basic SELECT queries.");
            return;
        }

        int fromIndex = Array.IndexOf(parts, "FROM");
        if (fromIndex == -1 || fromIndex + 1 >= parts.Length)
        {
            Console.WriteLine("Syntax Error: 'FROM' clause is missing.");
            return;
        }

        string tableName = parts[fromIndex + 1];
        if (!database.Tables.TryGetValue(tableName, out var table))
        {
            Console.WriteLine($"Table '{tableName}' does not exist.");
            return;
        }

        string whereClause = null;
        int whereIndex = Array.FindIndex(parts, p => p.Equals("WHERE", StringComparison.OrdinalIgnoreCase));
        if (whereIndex != -1)
        {
            whereClause = string.Join(" ", parts.Skip(whereIndex + 1));
        }

        stopwatch.Stop();
        Console.WriteLine("📊 EXPLAIN PLAN:");
        Console.WriteLine($"➡ Table: {tableName}");

        if (!string.IsNullOrEmpty(whereClause))
        {
            string normalizedWhere = Regex.Replace(whereClause, @"([^\s])=([^\s])", "$1 = $2");
            var whereParts = normalizedWhere.Split(' ', StringSplitOptions.RemoveEmptyEntries);

            if (whereParts.Length == 3 && whereParts[1] == "=")
            {
                string column = whereParts[0];
                string value = whereParts[2].Trim('\'', '"');

                if (table.Indexes.TryGetValue(column, out var index))
                {
                    var timer = System.Diagnostics.Stopwatch.StartNew();
                    var result = index.LookupRows(value);
                    timer.Stop();

                    Console.WriteLine($"✅ Access Type: INDEXED LOOKUP");
                    Console.WriteLine($"🔎 Index Used: {column}");
                    Console.WriteLine($"🔑 Search Key: '{value}'");
                    Console.WriteLine($"🕒 Lookup Time: {timer.Elapsed.TotalMilliseconds:F3} ms");
                    Console.WriteLine($"📦 Estimated Rows: {result?.Count ?? 0}");
                    Console.WriteLine($"✅ Parse Time: {stopwatch.Elapsed.TotalMilliseconds:F3} ms");
                    return;
                }
                else
                {
                    var timer = System.Diagnostics.Stopwatch.StartNew();
                    var result = table.Rows.Where(row => EvaluateWhereClause(row, whereClause)).ToList();
                    timer.Stop();

                    Console.WriteLine($"⚠️ Access Type: FULL TABLE SCAN");
                    Console.WriteLine($"🔎 Filter Column: {column}");
                    Console.WriteLine($"🕒 Scan Time: {timer.Elapsed.TotalMilliseconds:F3} ms");
                    Console.WriteLine($"📦 Estimated Rows: {result.Count}");
                    Console.WriteLine($"✅ Parse Time: {stopwatch.Elapsed.TotalMilliseconds:F3} ms");
                    return;
                }
            }
            else
            {
                var timer = System.Diagnostics.Stopwatch.StartNew();
                var result = table.Rows.Where(row => EvaluateWhereClause(row, whereClause)).ToList();
                timer.Stop();

                Console.WriteLine("⚠️ Complex WHERE clause; using full table scan.");
                Console.WriteLine($"🕒 Scan Time: {timer.Elapsed.TotalMilliseconds:F3} ms");
                Console.WriteLine($"📦 Estimated Rows: {result.Count}");
                Console.WriteLine($"✅ Parse Time: {stopwatch.Elapsed.TotalMilliseconds:F3} ms");
                return;
            }
        }
        else
        {
            var timer = System.Diagnostics.Stopwatch.StartNew();
            var result = table.Rows.ToList(); // Full scan
            timer.Stop();

            Console.WriteLine("⚠️ No WHERE clause; using full table scan.");
            Console.WriteLine($"🕒 Scan Time: {timer.Elapsed.TotalMilliseconds:F3} ms");
            Console.WriteLine($"📦 Estimated Rows: {result.Count}");
            Console.WriteLine($"✅ Parse Time: {stopwatch.Elapsed.TotalMilliseconds:F3} ms");
            return;
        }
    }

    public void HandleSelectQuery(string query,Guid id)
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

        // 1. Extract WHERE, GROUP BY, HAVING, and ORDER BY clauses if present
        string whereClause = null;
        string groupByClause = null;
        string havingClause = null;
        string orderByColumn = null;
        bool orderByDescending = false;

        int whereIndex = Array.FindIndex(parts, p => p.Equals("WHERE", StringComparison.OrdinalIgnoreCase));
        int groupByIndex = Array.FindIndex(parts, p => p.Equals("GROUP", StringComparison.OrdinalIgnoreCase));
        int havingIndex = Array.FindIndex(parts, p => p.Equals("HAVING", StringComparison.OrdinalIgnoreCase));
        int orderIndex = Array.FindIndex(parts, p => p.Equals("ORDER", StringComparison.OrdinalIgnoreCase));

        if (orderIndex != -1 && orderIndex + 2 < parts.Length && parts[orderIndex + 1].Equals("BY", StringComparison.OrdinalIgnoreCase))
        {
            orderByColumn = parts[orderIndex + 2];
            orderByDescending = orderIndex + 3 < parts.Length && parts[orderIndex + 3].Equals("DESC", StringComparison.OrdinalIgnoreCase);
        }

        if (groupByIndex != -1 && groupByIndex + 1 < parts.Length && parts[groupByIndex + 1].Equals("BY", StringComparison.OrdinalIgnoreCase))
        {
            int start = groupByIndex + 2;
            int end = (havingIndex != -1) ? havingIndex : (orderIndex != -1 ? orderIndex : parts.Length);
            groupByClause = string.Join(" ", parts.Skip(start).Take(end - start));
        }

        if (havingIndex != -1)
        {
            int start = havingIndex + 1;
            int end = (orderIndex != -1) ? orderIndex : parts.Length;
            havingClause = string.Join(" ", parts.Skip(start).Take(end - start));
        }

        if (whereIndex != -1)
        {
            int start = whereIndex + 1;
            int end = (groupByIndex == -1) ? (orderIndex == -1 ? parts.Length : orderIndex) : groupByIndex;
            whereClause = string.Join(" ", parts.Skip(start).Take(end - start));
        }

        // 2. Extract columns and table name
        int fromIndex = Array.IndexOf(parts, "FROM");
        if (fromIndex == -1)
        {
            Console.WriteLine("Syntax Error: 'FROM' keyword missing.");
            return;
        }

        var selectedColumns = parts.Skip(1).Take(fromIndex - 1).Where(p => p != ",").ToList();
        bool selectAll = selectedColumns.Count == 1 && selectedColumns[0] == "*";

        // 3. Handle JOIN query
        if (parts.Contains("JOIN"))
        {
            string table1 = parts[fromIndex + 1];
            string joinType = parts[fromIndex + 2].ToUpper() == "JOIN" ? "INNER" : parts[fromIndex + 2].ToUpper();
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

            var results = ExecuteJoinQuery(leftTable, rightTable, leftColumn, rightColumn, joinType,id, database);

            // Apply WHERE clause
            if (!string.IsNullOrEmpty(whereClause))
            {
                results = results.Where(row => EvaluateWhereClauseWithQuotes(row, whereClause)).ToList();
            }

            // Apply GROUP BY
            if (!string.IsNullOrEmpty(groupByClause))
            {
                results = ApplyGroupBy(results, groupByClause);
            }

            // Apply HAVING clause
            if (!string.IsNullOrEmpty(havingClause))
            {
                results = results.Where(row => EvaluateWhereClause(row, havingClause)).ToList();
            }

            // Apply ORDER BY
            if (!string.IsNullOrEmpty(orderByColumn))
            {
                results = ApplyOrderBy(results, orderByColumn, orderByDescending);
            }

            // Print results
            foreach (var row in results)
            {
                Console.WriteLine(string.Join(" | ", selectAll
                    ? row.Select(kv => $"{kv.Key}: {kv.Value}")
                    : row.Where(kv => selectedColumns.Contains(kv.Key)).Select(kv => $"{kv.Key}: {kv.Value}")));
            }

            return;
        }

        // 4. Handle Simple SELECT (no JOIN)
        string tableName = parts[fromIndex + 1];
        if (!database.Tables.TryGetValue(tableName, out var table))
        {
            Console.WriteLine($"Table '{tableName}' does not exist.");
            return;
        }

        var rows = transactionManager.GetVisibleRows(id, tableName, database);

        // Apply WHERE clause
        if (!string.IsNullOrEmpty(whereClause))
        {
            rows = rows.Where(row => EvaluateWhereClause(row, whereClause)).ToList();
        }

        // Apply GROUP BY
        if (!string.IsNullOrEmpty(groupByClause))
        {
            rows = ApplyGroupBy(rows, groupByClause);
        }

        // Apply HAVING clause
        if (!string.IsNullOrEmpty(havingClause))
        {
            rows = rows.Where(row => EvaluateWhereClause(row, havingClause)).ToList();
        }

        // Apply ORDER BY
        if (!string.IsNullOrEmpty(orderByColumn))
        {
            rows = ApplyOrderBy(rows, orderByColumn, orderByDescending);
        }

        // Print results
        foreach (var row in rows)
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

    private List<Dictionary<string, string>> ApplyOrderBy(List<Dictionary<string, string>> rows, string orderByColumn, bool descending)
    {
        return descending
            ? rows.OrderByDescending(r => r.ContainsKey(orderByColumn) ? r[orderByColumn] : null).ToList()
            : rows.OrderBy(r => r.ContainsKey(orderByColumn) ? r[orderByColumn] : null).ToList();
    }
    private List<Dictionary<string, string>> ApplyGroupBy(List<Dictionary<string, string>> rows, string groupByClause)
    {
        var groupByColumns = groupByClause.Split(',').Select(c => c.Trim()).ToList();
        var grouped = rows.GroupBy(row => string.Join("|", groupByColumns.Select(col => row[col])));

        var groupedRows = new List<Dictionary<string, string>>();

        foreach (var group in grouped)
        {
            var first = group.First();
            var groupedRow = new Dictionary<string, string>();

            // Add grouping key fields
            foreach (var col in groupByColumns)
            {
                groupedRow[col] = first[col];
            }

            // Example: aggregate COUNT
            groupedRow["COUNT"] = group.Count().ToString();

            groupedRows.Add(groupedRow);
        }

        return groupedRows;
    }

    private bool EvaluateWhereClause(Dictionary<string, string> row, string condition)
    {
        condition = condition.Replace(" not ", " NOT ", StringComparison.OrdinalIgnoreCase)
                             .Replace(" and ", " AND ", StringComparison.OrdinalIgnoreCase)
                             .Replace(" or ", " OR ", StringComparison.OrdinalIgnoreCase);

        var expr = condition.Split(new[] { " OR " }, StringSplitOptions.None);
        foreach (var orBlock in expr)
        {
            var ands = orBlock.Split(new[] { " AND " }, StringSplitOptions.None);
            bool allTrue = true;

            foreach (var clause in ands)
            {
                string trimmed = clause.Trim();

                bool negate = false;
                if (trimmed.StartsWith("NOT ", StringComparison.OrdinalIgnoreCase))
                {
                    negate = true;
                    trimmed = trimmed.Substring(4).Trim();
                }

                string[] operators = new[] { ">=", "<=", "!=", "=", ">", "<" };
                string op = operators.FirstOrDefault(o => trimmed.Contains(o));
                if (op == null)
                {
                    Console.WriteLine($"Unsupported operator in WHERE clause: {trimmed}");
                    return false;
                }

                var parts = trimmed.Split(new[] { op }, StringSplitOptions.None);
                string column = parts[0].Trim();
                string value = parts[1].Trim();
                value = value.Trim('\'', '"');


                if (!row.ContainsKey(column))
                    return false;

                string cell = row[column];

                int comparison = string.Compare(cell, value, StringComparison.OrdinalIgnoreCase);
                bool result = op switch
                {
                    "=" => cell == value,
                    "!=" => cell != value,
                    ">" => comparison > 0,
                    "<" => comparison < 0,
                    ">=" => comparison >= 0,
                    "<=" => comparison <= 0,
                    _ => false
                };

                if (negate) result = !result;

                if (!result)
                {
                    allTrue = false;
                    break;
                }
            }

            if (allTrue) return true;
        }

        return false;
    }


    private void HandleUpdateQuery(string[] parts)
    {
        string query = string.Join(" ", parts);
        string pattern = @"update\s+(\w+)\s+set\s+(.+?)\s+where\s+(.+?)(?:\s+order\s+by\s+(.+))?$";
        var match = Regex.Match(query, pattern, RegexOptions.IgnoreCase);

        if (!match.Success)
        {
            Console.WriteLine("Syntax Error: Use UPDATE <table> SET col=val,... WHERE condition [ORDER BY col ASC|DESC]");
            return;
        }

        string tableName = match.Groups[1].Value;
        string setClause = match.Groups[2].Value;
        string whereClause = match.Groups[3].Value;
        string orderByClause = match.Groups[4].Success ? match.Groups[4].Value : null;

        if (!database.Tables.ContainsKey(tableName))
        {
            Console.WriteLine($"Table '{tableName}' does not exist.");
            return;
        }

        var table = database.Tables[tableName];

        // Parse SET assignments properly
        Dictionary<string, string> updatedValues = ParseAssignments(setClause);

        // Filter rows matching the WHERE clause
        var matchingRows = table.Rows
            .Where(row => EvaluateWhereClauseWithQuotes(row, whereClause))
            .ToList();

        if (!matchingRows.Any())
        {
            Console.WriteLine("No matching rows found for update.");
            return;
        }

        var pkColumn = table.Columns.FirstOrDefault(c => c.Constraint.Has(ConstraintType.PrimaryKey));
        if (pkColumn == null)
        {
            Console.WriteLine("Primary key not defined.");
            return;
        }

        foreach (var row in matchingRows)
        {
            if (!row.TryGetValue(pkColumn.Name, out var primaryKeyValue))
            {
                Console.WriteLine("Missing primary key value.");
                continue;
            }

            

            try
            {
                table.UpdateRow(primaryKeyValue, updatedValues);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Update failed: {ex.Message}");
            }
        }

        Console.WriteLine($"{matchingRows.Count} row(s) updated in '{tableName}'.");
    }
    private void HandleUpdateQuery(string[] parts,Guid id)
    {
        string query = string.Join(" ", parts);
        string pattern = @"update\s+(\w+)\s+set\s+(.+?)\s+where\s+(.+?)(?:\s+order\s+by\s+(.+))?$";
        var match = Regex.Match(query, pattern, RegexOptions.IgnoreCase);

        if (!match.Success)
        {
            Console.WriteLine("Syntax Error: Use UPDATE <table> SET col=val,... WHERE condition [ORDER BY col ASC|DESC]");
            return;
        }

        string tableName = match.Groups[1].Value;
        string setClause = match.Groups[2].Value;
        string whereClause = match.Groups[3].Value;
        string orderByClause = match.Groups[4].Success ? match.Groups[4].Value : null;

        if (!database.Tables.ContainsKey(tableName))
        {
            Console.WriteLine($"Table '{tableName}' does not exist.");
            return;
        }

        var table = database.Tables[tableName];

        // Parse SET assignments properly
        Dictionary<string, string> updatedValues = ParseAssignments(setClause);

        // Filter rows matching the WHERE clause
        var matchingRows = table.Rows
            .Where(row => EvaluateWhereClauseWithQuotes(row, whereClause))
            .ToList();

        if (!matchingRows.Any())
        {
            Console.WriteLine("No matching rows found for update.");
            return;
        }

        var pkColumn = table.Columns.FirstOrDefault(c => c.Constraint.Has(ConstraintType.PrimaryKey));
        if (pkColumn == null)
        {
            Console.WriteLine("Primary key not defined.");
            return;
        }

        foreach (var row in matchingRows)
        {
            if (!row.TryGetValue(pkColumn.Name, out var primaryKeyValue))
            {
                Console.WriteLine("Missing primary key value.");
                continue;
            }

            // Log transaction
           
                var before = new Dictionary<string, string>(row);
                var after = new Dictionary<string, string>(row);
                foreach (var kvp in updatedValues)
                    after[kvp.Key] = kvp.Value;

            transactionManager.LogOperation(
                id,
                "update",
                tableName,
                new() { before },
                new() { after },
                database
            );
            

            //try
            //{
            //    table.UpdateRow(primaryKeyValue, updatedValues);
            //}
            //catch (Exception ex)
            //{
            //    Console.WriteLine($"Update failed: {ex.Message}");
            //}
        }

        Console.WriteLine($"{matchingRows.Count} row(s) updated in '{tableName}'.");
    }
    // Parses SET clause, handling quoted values properly
    private Dictionary<string, string> ParseAssignments(string input)
    {
        Dictionary<string, string> assignments = new();
        bool insideQuotes = false;
        string current = "";
        List<string> pairs = new();

        foreach (char c in input)
        {
            if (c == '"')
            {
                insideQuotes = !insideQuotes;
                current += c;
            }
            else if (c == ',' && !insideQuotes)
            {
                pairs.Add(current.Trim());
                current = "";
            }
            else
            {
                current += c;
            }
        }

        if (!string.IsNullOrEmpty(current))
            pairs.Add(current.Trim());

        foreach (var assignment in pairs)
        {
            var parts = assignment.Split('=');
            if (parts.Length != 2)
                throw new Exception("Invalid SET assignment format.");

            string column = parts[0].Trim();
            string value = parts[1].Trim().Trim('"'); // Remove surrounding quotes
            assignments[column] = value;
        }

        return assignments;
    }

    // Evaluate WHERE clause supporting quoted strings
    private bool EvaluateWhereClauseWithQuotes(Dictionary<string, string> row, string condition)
    {
        condition = condition.Replace(" not ", " NOT ", StringComparison.OrdinalIgnoreCase)
                             .Replace(" and ", " AND ", StringComparison.OrdinalIgnoreCase)
                             .Replace(" or ", " OR ", StringComparison.OrdinalIgnoreCase);

        var expr = condition.Split(new[] { " OR " }, StringSplitOptions.None);
        foreach (var orBlock in expr)
        {
            var ands = orBlock.Split(new[] { " AND " }, StringSplitOptions.None);
            bool allTrue = true;

            foreach (var clause in ands)
            {
                string trimmed = clause.Trim();

                bool negate = false;
                if (trimmed.StartsWith("NOT ", StringComparison.OrdinalIgnoreCase))
                {
                    negate = true;
                    trimmed = trimmed.Substring(4).Trim();
                }

                string[] operators = new[] { ">=", "<=", "!=", "=", ">", "<" };
                string op = operators.FirstOrDefault(o => trimmed.Contains(o));
                if (op == null)
                {
                    Console.WriteLine($"Unsupported operator in WHERE clause: {trimmed}");
                    return false;
                }

                var parts = trimmed.Split(new[] { op }, StringSplitOptions.None);
                if (parts.Length != 2)
                {
                    Console.WriteLine($"Invalid condition: {trimmed}");
                    return false;
                }

                string column = parts[0].Trim();
                string value = parts[1].Trim();

                // Remove surrounding quotes if present
                if ((value.StartsWith("\"") && value.EndsWith("\"")) || (value.StartsWith("'") && value.EndsWith("'")))
                {
                    value = value.Substring(1, value.Length - 2);
                }

                if (!row.ContainsKey(column))
                    return false;

                string cell = row[column];

                int comparison = string.Compare(cell, value, StringComparison.OrdinalIgnoreCase);
                bool result = op switch
                {
                    "=" => cell == value,
                    "!=" => cell != value,
                    ">" => comparison > 0,
                    "<" => comparison < 0,
                    ">=" => comparison >= 0,
                    "<=" => comparison <= 0,
                    _ => false
                };

                if (negate) result = !result;

                if (!result)
                {
                    allTrue = false;
                    break;
                }
            }

            if (allTrue) return true;
        }

        return false;
    }



    private void HandleDeleteQuery(string[] parts)
    {
        if (parts.Length < 4 || !parts[0].Equals("delete", StringComparison.OrdinalIgnoreCase) ||
            !parts[1].Equals("from", StringComparison.OrdinalIgnoreCase))
        {
            Console.WriteLine("Syntax Error: Use DELETE FROM <table_name> WHERE <condition>");
            return;
        }

        string tableName = parts[2];
        if (!database.Tables.ContainsKey(tableName))
        {
            Console.WriteLine($"Table '{tableName}' does not exist.");
            return;
        }

        var table = database.Tables[tableName];

        // Reconstruct full query to parse WHERE clause easily
        string fullQuery = string.Join(" ", parts);
        int whereIndex = fullQuery.IndexOf(" where ", StringComparison.OrdinalIgnoreCase);
        if (whereIndex == -1)
        {
            Console.WriteLine("Syntax Error: Missing WHERE clause.");
            return;
        }

        string condition = fullQuery.Substring(whereIndex + 7).Trim(); // after "WHERE "

        // Get primary key column
        var pkColumn = table.Columns.FirstOrDefault(c => c.Constraint.Has(ConstraintType.PrimaryKey));
        if (pkColumn == null)
        {
            Console.WriteLine("No primary key defined for the table.");
            return;
        }

        // Evaluate and collect rows to delete
        var rowsToDelete = table.Rows
            .Where(row => EvaluateWhereClauseWithQuotes(row, condition)) // <-- UPDATED HERE
            .ToList();

        if (rowsToDelete.Count == 0)
        {
            Console.WriteLine("No matching rows found for deletion.");
            return;
        }

        foreach (var row in rowsToDelete)
        {
            if (!row.ContainsKey(pkColumn.Name))
            {
                Console.WriteLine("Primary key value missing in a matching row. Skipping deletion for that row.");
                continue;
            }

            string pkValue = row[pkColumn.Name];

            // Transaction logging (before snapshot)
           

            try
            {
                table.DeleteRow(pkValue,transactionManager,null);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Delete failed for row with {pkColumn.Name} = {pkValue}: {ex.Message}");
            }
        }

        Console.WriteLine($"{rowsToDelete.Count} row(s) deleted successfully from '{tableName}'.");
    }

    private void HandleDeleteQuery(string[] parts,Guid id)
    {
        if (parts.Length < 4 || !parts[0].Equals("delete", StringComparison.OrdinalIgnoreCase) ||
            !parts[1].Equals("from", StringComparison.OrdinalIgnoreCase))
        {
            Console.WriteLine("Syntax Error: Use DELETE FROM <table_name> WHERE <condition>");
            return;
        }

        string tableName = parts[2];
        if (!database.Tables.ContainsKey(tableName))
        {
            Console.WriteLine($"Table '{tableName}' does not exist.");
            return;
        }

        var table = database.Tables[tableName];

        // Reconstruct full query to parse WHERE clause easily
        string fullQuery = string.Join(" ", parts);
        int whereIndex = fullQuery.IndexOf(" where ", StringComparison.OrdinalIgnoreCase);
        if (whereIndex == -1)
        {
            Console.WriteLine("Syntax Error: Missing WHERE clause.");
            return;
        }

        string condition = fullQuery.Substring(whereIndex + 7).Trim(); // after "WHERE "

        // Get primary key column
        var pkColumn = table.Columns.FirstOrDefault(c => c.Constraint.Has(ConstraintType.PrimaryKey));
        if (pkColumn == null)
        {
            Console.WriteLine("No primary key defined for the table.");
            return;
        }

        // Evaluate and collect rows to delete
        var rowsToDelete = table.Rows
            .Where(row => EvaluateWhereClauseWithQuotes(row, condition)) // <-- UPDATED HERE
            .ToList();

        if (rowsToDelete.Count == 0)
        {
            Console.WriteLine("No matching rows found for deletion.");
            return;
        }

        foreach (var row in rowsToDelete)
        {
            if (!row.ContainsKey(pkColumn.Name))
            {
                Console.WriteLine("Primary key value missing in a matching row. Skipping deletion for that row.");
                continue;
            }

            string pkValue = row[pkColumn.Name];

            // Transaction logging (before snapshot)

            //var beforeSnapshot = new Dictionary<string, string>(row);
            //transactionManager.LogOperation(
            //    id,
            //    "delete",
            //    tableName,
            //    new List<Dictionary<string, string>> { beforeSnapshot },
            //    null,
            //    database
            //);


            try
            {
                table.DeleteRow(pkValue, transactionManager, id);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Delete failed for row with {pkColumn.Name} = {pkValue}: {ex.Message}");
            }
        }

        Console.WriteLine($"{rowsToDelete.Count} row(s) deleted successfully from '{tableName}'.");
    }



}
