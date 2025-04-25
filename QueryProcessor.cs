using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using ConsoleApp1;

class QueryProcessor
{
    private Database database;
    private TransactionManager transactionManager;
    private readonly DBMS dbms;
    private Guid? currentTransactionId = null; 

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

            // Create the table
            database.CreateTable(tableName, columns);
            Console.WriteLine($"Table '{tableName}' created successfully in database '{database.Name}'.");

            // ✅ Transaction Logging for CREATE TABLE
            if (currentTransactionId.HasValue)
            {
                transactionManager.LogOperation(
                    currentTransactionId.Value,
                    "create",
                    tableName,
                    null,
                    null,
                    database
                );
            }
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
                results = results.Where(row => EvaluateWhereClause(row, whereClause)).ToList();
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

        // Parse SET assignments
        Dictionary<string, string> updatedValues = new();
        foreach (var assignment in setClause.Split(','))
        {
            var pair = assignment.Split('=');
            if (pair.Length != 2)
            {
                Console.WriteLine("Invalid SET clause format.");
                return;
            }
            updatedValues[pair[0].Trim()] = pair[1].Trim();
        }

        // Filter rows matching the WHERE clause
        var matchingRows = table.Rows
            .Where(row => EvaluateWhereClause(row, whereClause))
            .ToList();

        if (!matchingRows.Any())
        {
            Console.WriteLine("No matching rows found for update.");
            return;
        }

        // Apply ORDER BY if specified
        //if (!string.IsNullOrEmpty(orderByClause))
        //{
        //    matchingRows = ApplyOrderBy(matchingRows, orderByClause);
        //}

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
            if (currentTransactionId.HasValue)
            {
                var before = new Dictionary<string, string>(row);
                var after = new Dictionary<string, string>(row);
                foreach (var kvp in updatedValues)
                    after[kvp.Key] = kvp.Value;

                transactionManager.LogOperation(
                    currentTransactionId.Value,
                    "update",
                    tableName,
                    new() { before },
                    new() { after },
                    database
                );
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
            .Where(row => EvaluateWhereClause(row, condition))
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
            if (currentTransactionId.HasValue)
            {
                var beforeSnapshot = new Dictionary<string, string>(row);
                transactionManager.LogOperation(
                    currentTransactionId.Value,
                    "delete",
                    tableName,
                    new List<Dictionary<string, string>> { beforeSnapshot },
                    null,
                    database
                );
            }

            try
            {
                table.DeleteRow(pkValue);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Delete failed for row with {pkColumn.Name} = {pkValue}: {ex.Message}");
            }
        }

        Console.WriteLine($"{rowsToDelete.Count} row(s) deleted successfully from '{tableName}'.");
    }



}
