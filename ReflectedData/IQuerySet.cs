
#region using

using System.Collections.Generic;
// ReSharper disable UnusedMember.Global
// ReSharper disable InconsistentNaming

#endregion

namespace ReflectedData
{
    /// <summary>
    ///     Known aggregate sql functions
    /// </summary>
    public enum SqlFunction
    {
        Avg,
        Count,
        First,
        Last,
        Max,
        Min,
        Sum
    }

    public class GroupResult
    {
        public object extraField;
        public decimal functionField;
        public object groupedByField;
    }

    public interface IQuerySet<T> : IEnumerable<T>
        where T : class, new()
    {
        int Count { get; }
        T First { get; }
        decimal Function(SqlFunction f, string overField);

        IEnumerable<GroupResult> Group(string byField, string extraField, SqlFunction f, string functionField,
            string havingSection);

        IEnumerable<T> Like(string indexValuePattern);
        IEnumerable<T> Query(string criteria);
        IEnumerable<T> QueryByField(string fieldName, object v);
        int QueryCount(string criteria);
        IQuerySet<T> Sort(string fieldsList);
        IQuerySet<T> Subset(string moreCriteria);
        decimal Sum(string overField);
        List<T> ToList();
    }
}