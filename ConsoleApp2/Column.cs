using System;
using System.Runtime.Serialization;

namespace ConsoleApp1
{
    [DataContract]
    class Column
    {
        [DataMember]
        public string Name { get; set; }
        [DataMember]
        public string Data_Type { get; set; }
        [DataMember]// e.g., "INT", "STRING", "BOOLEAN"
        public int? MaxLength { get; set; } // optional, used for STRING
        [DataMember]
        public ColumnConstraint Constraint { get; set; }

        [IgnoreDataMember]
        private Table ParentTable;

        public Table ParentDatabase
        {
            get => ParentTable;
            set => ParentTable = value;
        }



        // B-Tree index for the column (only for indexed columns)
        public BTree Index { get; set; }

        // Parameterless constructor needed for deserialization
        public Column() { }

        // Constructor for defining columns with or without indexes
        public Column(string name, string type, int? maxLength = null, ColumnConstraint constraint = null, bool createIndex = false)
        {
            Name = name;
            Data_Type = type.ToUpper();
            MaxLength = maxLength;
            Constraint = constraint ?? new ColumnConstraint();

            // If createIndex is true, initialize BTree index
            if (createIndex)
            {
                // Create BTree index with degree 3 (can be adjusted)
                Index = new BTree(3);
            }
        }

        // Method to set parent table (helps for later referencing)
        public void SetParentTable(Table table)
        {
            this.ParentTable = table;
        }

        // Optional: Method to insert data into the column index if it's indexed
        public void InsertIndex(IComparable key, object value)
        {
            if (Index != null)
            {
                Index.Insert(key, value);
            }
        }

        // Optional: Search for data in the index (if available)
        public object SearchInIndex(IComparable key)
        {
            if (Index != null)
            {
                return Index.Search(key);
            }
            return null;
        }
    }
}