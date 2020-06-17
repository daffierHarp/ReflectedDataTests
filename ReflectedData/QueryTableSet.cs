#region using

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

#endregion

namespace ReflectedData
{
    /// <summary>
    ///     A collection over a query, with optional order-by - over a single table.
    ///     Add is not unsupported (only in subclasses like JoinSet)
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class QueryTableSet<T> : ICollection<T>, IQuerySet<T>
        where T : class, new()
    {
        readonly string _sectionWhere;
        readonly string _sectionOrderBy;

        internal QueryTableSet(string sectionWhere, string sectionOrderBy, ReflectedTable<T> table)
        {
            this._sectionWhere = sectionWhere;
            this._sectionOrderBy = sectionOrderBy;
            Table = table;
        }

        public ReflectedTable<T> Table { get; }

        public virtual int Count => Table.CountQuery(GetCriteria());

        #region IEnumerable<T> Members

        public IEnumerator<T> GetEnumerator()
        {
            var sql = Table.BuildSelectSql(-1, GetCriteria(), _sectionOrderBy);
            var r = Table.MyDataSource.ExecuteReader(sql);
            return new DataTableReaderEnumerator<T>(Table.MyDataSource, Table, r);
        }

        #endregion

        #region IEnumerable Members

        IEnumerator IEnumerable.GetEnumerator()
        {
            var sql = Table.BuildSelectSql(-1, GetCriteria(), _sectionOrderBy);
            var r = Table.MyDataSource.ExecuteReader(sql);
            return new DataTableReaderEnumerator<T>(Table.MyDataSource, Table, r);
        }

        #endregion

        /// <summary>
        ///     return a set (without the ability to add new lines) at which the lines will
        ///     be enumerated by the order of the requested fields
        /// </summary>
        /// <param name="fieldsList"></param>
        /// <returns></returns>
        public IQuerySet<T> Sort(string fieldsList) => new QueryTableSet<T>(GetCriteria(), fieldsList, Table);

        /// <summary>
        ///     Return a set (without the ability to add new lines) at which the lines will meet current
        ///     criteria and the new new criteria
        /// </summary>
        /// <param name="moreCriteria"></param>
        /// <returns></returns>
        public IQuerySet<T> Subset(string moreCriteria)
        {
            if (moreCriteria + "" == "")
                return this;
            return new QueryTableSet<T>(GetCriteria() + " and (" + moreCriteria + ")", _sectionOrderBy, Table);
        }

        public decimal Function(SqlFunction f, string overField)
        {
            var sb = new StringBuilder();
            sb.Append("SELECT " + f.ToString().ToUpper() + "(" + overField + ")");
            sb.Append(" FROM " + Table.TableName);
            sb.Append(" WHERE " + GetCriteria());
            sb.Append(";");
            return
                Convert.ToDecimal(
                    Table.MyDataSource.ExecuteScalarSqlToObject(sb.ToString()));
        }

        public decimal Sum(string overField) => Function(SqlFunction.Sum, overField);

        public IEnumerable<GroupResult> Group(string byField, string extraField, SqlFunction f, string functionField,
            string havingSection)
        {
            var sb = new StringBuilder();
            sb.Append("SELECT " + byField);
            if (extraField + "" != "")
                sb.Append(", " + extraField);
            sb.Append(", " + f.ToString().ToUpper() + "(" + functionField + ")");
            sb.Append(" FROM " + Table.TableName);
            sb.Append(" WHERE " + GetCriteria());
            sb.Append(" GROUP BY " + byField);
            if (extraField + "" != "")
                sb.Append(", " + extraField);
            if (havingSection + "" != "")
                sb.Append(" HAVING " + havingSection);
            sb.Append(";");
            var sql = sb.ToString();
            var fieldsNames = new List<string> {"groupedByField", "functionField"};
            if (extraField + "" != "")
                fieldsNames.Insert(1, "extraField");
            var fields = (from fn in fieldsNames
                select typeof(GroupResult).GetField(fn)).ToList();
            var result =
                new QueryNonTableEnumerable<GroupResult>
                    (Table.MyDataSource, fields, sql);
            return result;
        }

        public IEnumerable<T> QueryByField(string fieldName, object v) =>
            Table.Select(-1, GetCriteria() + " and " + fieldName + "=" +
                             DataSource.ValueToSql(v), _sectionOrderBy);

        public IEnumerable<T> Query(string criteria) =>
            Table.Select(-1, GetCriteria() + " and (" + criteria + ")", _sectionOrderBy);

        public int QueryCount(string criteria) => Table.CountQuery(GetCriteria() + " and (" + criteria + ")");

        public IEnumerable<T> Like(string indexValuePattern) => Table.Select(-1,
            GetCriteria() + " and " + Table.GetLikeCriteria(indexValuePattern), _sectionOrderBy);

        public T First => Table.SelectFirst(GetCriteria());

        public List<T> ToList() => Table.SelectList(-1, GetCriteria(), _sectionOrderBy);

        protected virtual string GetCriteria() => _sectionWhere;

        #region ICollection<T> Members

        public virtual void Add(T item)
        {
            throw new NotSupportedException();
        }

        public virtual void Clear()
        {
            Table.MyDataSource.Delete(Table.TableName, GetCriteria());
        }

        public virtual bool Contains(T item)
        {
            var itemId = Table.idFieldInfo?.GetValue(item);
            if (itemId is int id && id > 0) {
                var askedItem = Table.Get(id);
                return askedItem != null;
            }

            Table.GetInstanceValues(item, out _, out var fieldsNames, out var fieldsValues);
            var criteriaBuilder = new StringBuilder();
            for (var i = 0; i < fieldsNames.Length; i++) {
                if (i > 0)
                    criteriaBuilder.Append(" and ");
                criteriaBuilder.Append(fieldsNames[i]);
                criteriaBuilder.Append("=");
                criteriaBuilder.Append(DataSource.ValueToSql(fieldsValues[i]));
            }

            var askedItem2 = Table.SelectFirst(criteriaBuilder.ToString());
            return askedItem2 != null;
        }

        public virtual void CopyTo(T[] array, int arrayIndex)
        {
            var list = Table.SelectList(-1, GetCriteria(), _sectionOrderBy);
            list.CopyTo(array, arrayIndex);
        }

        public virtual bool IsReadOnly => false;

        public virtual bool Remove(T item) => Table.Delete(item);

        #endregion
    }
}