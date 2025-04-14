using System.Threading;
/// <summary>
/// This class implements the locks on table when it is being used by the transaction
/// I am not using the existing tables I have made a custom tablefortest class just to imlplement and check 
/// 
/// </summary>
public class ConcurrencyControl
{
    private Dictionary<string, ReaderWriterLockSlim> tableLocks = new();
    private Dictionary<Guid, List<ReaderWriterLockSlim>> transactionLocks = new();

    private ReaderWriterLockSlim GetLock(string tableName)
    {
        if (!tableLocks.ContainsKey(tableName))
            tableLocks[tableName] = new ReaderWriterLockSlim();
        return tableLocks[tableName];
    }

    public void AcquireWriteLock(string tableName, Guid txId)
    {
        var lockObj = GetLock(tableName);
        lockObj.EnterWriteLock();
        if (!transactionLocks.ContainsKey(txId))
            transactionLocks[txId] = new List<ReaderWriterLockSlim>();
        transactionLocks[txId].Add(lockObj);
        Console.WriteLine($"Write lock acquired on {tableName} by TX {txId}");
    }

    public void ReleaseLocks(Guid txId)
    {
        if (!transactionLocks.ContainsKey(txId)) return;

        foreach (var l in transactionLocks[txId])
        {
            if (l.IsWriteLockHeld) l.ExitWriteLock();
        }
        transactionLocks.Remove(txId);
        Console.WriteLine($"Locks released for TX {txId}");
    }
}
