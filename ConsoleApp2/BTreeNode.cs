public class BTreeNode
{
    public IComparable[] Keys { get; set; }
    public object[] Values { get; set; }
    public BTreeNode[] Children { get; set; }
    public int KeyCount { get; set; }
    public bool IsLeaf { get; set; }

    private readonly int degree;

    public BTreeNode(int degree, bool isLeaf)
    {
        this.degree = degree;
        IsLeaf = isLeaf;
        Keys = new IComparable[2 * degree - 1];
        Values = new object[2 * degree - 1];
        Children = new BTreeNode[2 * degree];
        KeyCount = 0;
    }

    public void InsertNonFull(IComparable key, object value)
    {
        int i = KeyCount - 1;

        if (IsLeaf)
        {
            while (i >= 0 && Keys[i].CompareTo(key) > 0)
            {
                Keys[i + 1] = Keys[i];
                Values[i + 1] = Values[i];
                i--;
            }

            Keys[i + 1] = key;
            Values[i + 1] = value;
            KeyCount++;
        }
        else
        {
            while (i >= 0 && Keys[i].CompareTo(key) > 0)
                i--;

            if (Children[i + 1].KeyCount == 2 * degree - 1)
            {
                SplitChild(i + 1, Children[i + 1]);

                if (Keys[i + 1].CompareTo(key) < 0)
                    i++;
            }

            Children[i + 1].InsertNonFull(key, value);
        }
    }

    public void SplitChild(int index, BTreeNode y)
    {
        var z = new BTreeNode(degree, y.IsLeaf)
        {
            KeyCount = degree - 1
        };

        for (int j = 0; j < degree - 1; j++)
        {
            z.Keys[j] = y.Keys[j + degree];
            z.Values[j] = y.Values[j + degree];
        }

        if (!y.IsLeaf)
        {
            for (int j = 0; j < degree; j++)
                z.Children[j] = y.Children[j + degree];
        }

        for (int j = KeyCount; j >= index + 1; j--)
            Children[j + 1] = Children[j];

        Children[index + 1] = z;

        for (int j = KeyCount - 1; j >= index; j--)
        {
            Keys[j + 1] = Keys[j];
            Values[j + 1] = Values[j];
        }

        Keys[index] = y.Keys[degree - 1];
        Values[index] = y.Values[degree - 1];
        KeyCount = KeyCount + 1;

        y.KeyCount = degree - 1;
    }

    public void Remove(IComparable key, object value)
    {
        int i = 0;
        while (i < KeyCount && Keys[i].CompareTo(key) < 0)
            i++;

        if (i < KeyCount && Keys[i].CompareTo(key) == 0)
        {
            if (IsLeaf)
            {
                // If the list associated with this key is now empty, remove the key
                if (value is List<Dictionary<string, string>> valList && valList.Count == 0)
                {
                    for (int j = i; j < KeyCount - 1; j++)
                    {
                        Keys[j] = Keys[j + 1];
                        Values[j] = Values[j + 1];
                    }

                    Keys[KeyCount - 1] = null;
                    Values[KeyCount - 1] = null;
                    KeyCount--;
                    Console.WriteLine("Removed");
                }
            }
        }
        else if (!IsLeaf)
        {
            Children[i].Remove(key, value);
        }
    }




    public object Search(IComparable key)
    {
        for (int i = 0; i < KeyCount; i++)
        {
            Console.WriteLine($"🔍 Comparing '{key}' to '{Keys[i]}'");
            if (key.CompareTo(Keys[i]) == 0)
            {
                Console.WriteLine($"✅ Match found for key '{key}'");
                return Values[i];
            }
        }

        return IsLeaf ? null : Children[KeyCount - 1].Search(key);
    }

    public void PrintKeys()
    {
        for (int i = 0; i < KeyCount; i++)
        {
            if (!IsLeaf) Children[i].PrintKeys();
            Console.WriteLine($"🔑 {Keys[i]}");
        }
        if (!IsLeaf) Children[KeyCount].PrintKeys();
    }

}
