using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

public class ConcurrencyControl
{
    private readonly object lockObj = new();
    private readonly Dictionary<(string table, int rowId), (ReaderWriterLockSlim lck, Guid? writeOwner, HashSet<Guid> readOwners)> locks = new();

    public void AcquireReadLock(string table, int rowId, Guid txId)
    {
        var key = (table, rowId);
        lock (lockObj)
        {
            if (!locks.ContainsKey(key))
            {
                locks[key] = (new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion), null, new HashSet<Guid>());
            }
        }

        var (lck, writeOwner, readOwners) = locks[key];

        // If this transaction owns the write lock, skip read lock
        if (writeOwner == txId)
            return;

        lck.EnterReadLock();
        lock (lockObj)
        {
            locks[key] = (lck, writeOwner, readOwners.Append(txId).ToHashSet());
        }
    }

    public void AcquireWriteLock(string table, int rowId, Guid txId)
    {
        var key = (table, rowId);
        lock (lockObj)
        {
            if (!locks.ContainsKey(key))
            {
                locks[key] = (new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion), null, new HashSet<Guid>());
            }
        }

        var (lck, writeOwner, readOwners) = locks[key];

        lck.EnterWriteLock();
        lock (lockObj)
        {
            locks[key] = (lck, txId, readOwners);
        }
    }
    public void AcquireUpgradeableReadLock(string table, int rowId, Guid txId)
    {
        var key = (table, rowId);
        lock (lockObj)
        {
            if (!locks.ContainsKey(key))
            {
                locks[key] = (new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion), null, new HashSet<Guid>());
            }
        }

        var (lck, writeOwner, readOwners) = locks[key];
        lck.EnterUpgradeableReadLock();

        lock (lockObj)
        {
            locks[key] = (lck, writeOwner, readOwners.Append(txId).ToHashSet());
        }
    }


    public void ReleaseLocks(Guid txId)
    {
        lock (lockObj)
        {
            foreach (var key in locks.Keys.ToList())
            {
                var (lck, writeOwner, readOwners) = locks[key];

                if (writeOwner == txId && lck.IsWriteLockHeld)
                {
                    lck.ExitWriteLock();
                    writeOwner = null;
                }

                if (readOwners.Contains(txId))
                {
                    if (lck.IsReadLockHeld)
                        lck.ExitReadLock();
                    readOwners.Remove(txId);
                }

                locks[key] = (lck, writeOwner, readOwners);
            }
        }
    }
}
