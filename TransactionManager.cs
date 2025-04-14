using System;
using System.Collections.Generic;
using System.Linq;

public class TransactionManager
{
    private Dictionary<Guid, Transaction> activeTransactions = new();
    private MVCCDatabase database;
    private ConcurrencyControl concurrencyControl;

    public TransactionManager(MVCCDatabase db, ConcurrencyControl cc)
    {
        database = db;
        concurrencyControl = cc;
    }

    public Guid BeginTransaction()
    {
        var txId = Guid.NewGuid();
        var tx = new Transaction(txId);
        activeTransactions[txId] = tx;
        Console.WriteLine($"Transaction {txId} started.");
        return txId;
    }

    public void Insert(Guid txId, string tableName, Dictionary<string, string> row)
    {
        concurrencyControl.AcquireWriteLock(tableName, txId);
        if (!activeTransactions.ContainsKey(txId)) return;

        var tx = activeTransactions[txId];
        database.Insert(tx, tableName, row);
    }

    public List<Dictionary<string, string>> Read(Guid txId, string tableName)
    {
        if (!activeTransactions.ContainsKey(txId)) return new();

        var tx = activeTransactions[txId];
        return database.Read(tx, tableName);
    }

    public void Commit(Guid txId)
    {
        if (!activeTransactions.ContainsKey(txId)) return;

        database.Commit(activeTransactions[txId]);
        concurrencyControl.ReleaseLocks(txId);
        activeTransactions.Remove(txId);
        Console.WriteLine($"Transaction {txId} committed.");
    }

    public void Rollback(Guid txId)
    {
        if (!activeTransactions.ContainsKey(txId)) return;

        database.Rollback(activeTransactions[txId]);
        concurrencyControl.ReleaseLocks(txId);
        activeTransactions.Remove(txId);
        Console.WriteLine($"Transaction {txId} rolled back.");
    }
}

public class Transaction
{
    public Guid Id { get; }
    public Dictionary<string, List<Dictionary<string, string>>> UncommittedChanges = new();

    public Transaction(Guid id)
    {
        Id = id;
    }
}