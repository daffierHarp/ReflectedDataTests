
#region using

using System;
// ReSharper disable UnusedMember.Global

#endregion

namespace ReflectedData
{
    [AttributeUsage(AttributeTargets.Field, AllowMultiple = true)]
    public class DataFieldAttribute : Attribute
    {
        public bool Ignore { get; set; }
        public bool IsID { get; set; }
        public bool IsAutoIncrement { get; set; } = true;

        /// <summary>
        ///     Allow accessing a line in the table through table[value], where value would refer
        ///     to the indexed field. Only one field is the index field.
        /// </summary>
        public bool IsIndex { get; set; }

        public string Rename { get; set; }

        /// <summary>
        ///     Allow overriding default assignment of data types when creating tables from reflection
        /// </summary>
        public string SqlDataType { get; set; }
    }

    public class DataIndexFieldAttribute : DataFieldAttribute
    {
        public DataIndexFieldAttribute()
        {
            IsIndex = true;
        }
    }

    public class DataIdFieldAttribute : DataFieldAttribute
    {
        public DataIdFieldAttribute()
        {
            IsID = true;
        }
    }

    /// <summary>
    ///     define the field as an ignore data field, which the FillJoins function
    ///     will fetch as a connection from field at current class to field at other table
    /// </summary>
    public class DataJoinToOne : DataFieldAttribute
    {
        public DataJoinToOne(string thisFieldName)
        {
            Ignore = true;
            ThisFieldName = thisFieldName;
        }

        /// <summary>
        ///     the other table field name. If not specified, assumed the ID as deducted from type of field
        /// </summary>
        public string OtherTableField { get; set; }

        /// <summary>
        ///     this class's field name (not the table) for which value to lookup at joined other table
        /// </summary>
        public string ThisFieldName { get; set; }
    }

    /// <summary>
    ///     define the field as an ignore data field, which the FillJoins function
    ///     will fetch as a connection from field at current class to field at lines of table.
    ///     The type of the field marked by DataJoinToMany is expected to be a List of line type
    /// </summary>
    public class DataJoinToMany : DataFieldAttribute
    {
        /// <summary>
        ///     The fields are assumed to be this table's ID field and the other table will refer to
        ///     the same field name
        /// </summary>
        public DataJoinToMany()
        {
            Ignore = true;
        }

        /// <summary>
        ///     the other table field name.
        /// </summary>
        public string OtherTableField { get; set; }

        /// <summary>
        ///     this class's field name (not the table) for which value to lookup at joined other table
        /// </summary>
        public string ThisFieldName { get; set; }
    }
}