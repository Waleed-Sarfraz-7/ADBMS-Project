public class ConcurrencyControl
{
    private readonly object lockObj = new();
    private readonly Dictionary<(string table, int rowId), (ReaderWriterLockSlim lck, Guid? writeOwner, HashSet<Guid> readOwners)> locks = new();
    private readonly Dictionary<(string table, int rowId), Guid> deletedMarkers = new(); // NEW: tracks delete visibility

    public void AcquireReadLock(string table, int rowId, Guid txId)
    {
        var key = (table, rowId);
        lock (lockObj)
        {
            if (!locks.ContainsKey(key))
                locks[key] = (new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion), null, new HashSet<Guid>());
        }

        var (lck, writeOwner, readOwners) = locks[key];

        if (writeOwner == txId)
            return;

        lck.EnterReadLock();
        lock (lockObj)
        {
            readOwners.Add(txId);
            locks[key] = (lck, writeOwner, readOwners);
        }
    }

    public void AcquireWriteLock(string table, int rowId, Guid txId)
    {
        var key = (table, rowId);

        while (true)
        {
            lock (lockObj)
            {
                if (!locks.ContainsKey(key))
                    locks[key] = (new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion), null, new HashSet<Guid>());


                var (lck, writeOwner, readOwners) = locks[key];

                bool canAcquire = (writeOwner == null || writeOwner == txId) &&
                                  (readOwners.Count == 0 || (readOwners.Count == 1 && readOwners.Contains(txId)));

                if (canAcquire)
                {
                    lck.EnterWriteLock();
                    locks[key] = (lck, txId, readOwners); // Set write owner
                    return;
                }
            }

            // 🔁 Wait a bit then retry to avoid deadlock
            Thread.Sleep(100); // Backoff retry
        }
    }






    public void AcquireUpgradeableReadLock(string table, int rowId, Guid txId)
    {
        var key = (table, rowId);
        lock (lockObj)
        {
            if (!locks.ContainsKey(key))
                locks[key] = (new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion), null, new HashSet<Guid>());
        }

        var (lck, writeOwner, readOwners) = locks[key];
        lck.EnterUpgradeableReadLock();
        lock (lockObj)
        {
            readOwners.Add(txId);
            locks[key] = (lck, writeOwner, readOwners);
        }
    }

    // ✅ Mark a row as logically deleted by a transaction
    public void MarkRowAsDeleted(string table, int rowId, Guid txId)
    {
        lock (lockObj)
        {
            deletedMarkers[(table, rowId)] = txId;
        }
    }

    // ✅ Check if a row is marked as deleted by another transaction
    public bool IsRowDeleted(string table, int rowId, Guid checkingTxId)
    {
        lock (lockObj)
        {
            return deletedMarkers.TryGetValue((table, rowId), out var deleterTxId)
                   && deleterTxId != checkingTxId;
        }
    }

    // 🔁 Release locks + clean up deleted row markers
    public void ReleaseLocks(Guid txId)
    {
        lock (lockObj)
        {
            foreach (var key in locks.Keys.ToList())
            {
                var (lck, writeOwner, readOwners) = locks[key];

                if (writeOwner == txId && lck.IsWriteLockHeld && lck.RecursiveWriteCount > 0)
                {
                    lck.ExitWriteLock();
                    writeOwner = null;
                }

                if (readOwners.Contains(txId) && lck.IsReadLockHeld && lck.RecursiveReadCount > 0)
                {
                    lck.ExitReadLock();
                    readOwners.Remove(txId);
                }


                locks[key] = (lck, writeOwner, readOwners);
            }

            // ✅ Clean up deleted markers held by this transaction
            foreach (var marker in deletedMarkers.Where(kv => kv.Value == txId).Select(kv => kv.Key).ToList())
            {
                deletedMarkers.Remove(marker);
            }

            Monitor.PulseAll(lockObj);
        }
    }


}
