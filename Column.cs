using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp1
{
    class Column
    {
        public string Name { get; set; }
        public string Data_Type { get; set; } // e.g., "INT", "STRING", "BOOLEAN"
        public int? MaxLength { get; set; } // optional, used for STRING
        public ColumnConstraint Constraint { get; set; }
        public Table ParentTable { get; set; }

        public Column()
        {

        } // Parameterless constructor needed for deserialization

        public Column(string name, string type, int? maxLength = null, ColumnConstraint constraint = null)
        {
            Name = name;
            Data_Type = type.ToUpper();
            MaxLength = maxLength;
            Constraint = constraint ?? new ColumnConstraint();
        }
        public void SetParentTable(Table table)
        {
            this.ParentTable = table;
        }



    }

}
