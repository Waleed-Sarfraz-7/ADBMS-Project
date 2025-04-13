using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp1
{
    public enum ConstraintType
    {
        None = 0,
        NotNull = 1,
        Unique = 2,
        PrimaryKey = 4,
        ForeignKey = 8,
        Check = 16
    }

    public class ColumnConstraint
    {
        public ConstraintType Constraints { get; set; } = ConstraintType.None;
        public string DefaultValue { get; set; }
        public string CheckExpression { get; set; }

        public string ReferenceTable { get; set; }
        public string ReferenceColumn { get; set; }

        public bool Has(ConstraintType type) => Constraints.HasFlag(type);
    }

}
