using System;
using System.Runtime.Serialization;

namespace ConsoleApp1
{
    [Flags]
    public enum ConstraintType
    {
        None = 0,
        NotNull = 1,
        Unique = 2,
        PrimaryKey = 4,
        ForeignKey = 8,
        Check = 16
    }

    [DataContract]
    public class ColumnConstraint
    {
        [DataMember]
        public ConstraintType Constraints { get; set; } = ConstraintType.None;
        [DataMember]
        public string DefaultValue { get; set; }
        [DataMember]
        public string CheckExpression { get; set; }
        [DataMember]
        public string ReferenceTable { get; set; }
        [DataMember]
        public string ReferenceColumn { get; set; }

        public bool Has(ConstraintType type) => Constraints.HasFlag(type);
    }
}
