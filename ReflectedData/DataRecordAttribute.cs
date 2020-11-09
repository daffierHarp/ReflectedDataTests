
#region using

using System;

#endregion

namespace ReflectedData
{
    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = true)]
    public class DataRecordAttribute : Attribute
    {
        public DataRecordAttribute(bool allFields)
        {
            AllFields = allFields;
        }

        /// <summary>
        ///     Treat all public fields as data fields
        /// </summary>
        public bool AllFields { get; set; }

        public string IDField { get; set; }
        public string IndexField { get; set; }
        public string TableName { get; set; }

        /// <summary>
        ///     If IDField is not null, allows specifying that it is not auto incrementing and thus must be specified on an insert operation
        /// </summary>
        public bool AutoIncrement { get; set; } = true;
    }
}