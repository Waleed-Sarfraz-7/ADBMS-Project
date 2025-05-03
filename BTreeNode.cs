public class BTreeNode
{
    public IComparable[] Keys { get; set; }
    public object[] Values { get; set; }
    public BTreeNode[] Children { get; set; }
    public int KeyCount { get; set; }
    public bool IsLeaf { get; set; }
    private int degree;

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
        var z = new BTreeNode(degree, y.IsLeaf);
        z.KeyCount = degree - 1;

        for (int j = 0; j < degree - 1; j++)
        {
            z.Keys[j] = y.Keys[j + degree];
            z.Values[j] = y.Values[j + degree];
        }

        if (!y.IsLeaf)
        {
            for (int j = 0; j < degree; j++)
            {
                z.Children[j] = y.Children[j + degree];
            }
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
        KeyCount++;

        y.KeyCount = degree - 1;
    }

    public object Search(IComparable key)
    {
        int i = 0;
        while (i < KeyCount && key.CompareTo(Keys[i]) > 0)
            i++;

        if (i < KeyCount && key.CompareTo(Keys[i]) == 0)
            return Values[i];

        if (IsLeaf)
            return null;

        return Children[i].Search(key);
    }
}
