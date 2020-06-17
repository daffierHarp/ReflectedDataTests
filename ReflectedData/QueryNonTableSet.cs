#region using

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

#endregion

namespace ReflectedData
{
    /// <summary>
    ///     the generic type represents the reflect-able relation line after whatever magic happened in the from
    ///     section
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class QueryNonTableSet<T> : IQuerySet<T>
        where T : class, new()
    {
        protected readonly DataSource Src;
        protected readonly string WhereSection, OrderBySection;
        readonly List<FieldInfo> _fieldsList;
        readonly List<string> _fieldsNames;
        readonly bool _typeIsAttributed;

        public QueryNonTableSet(DataSource src,
            bool typeIsAttributed,
            string fromSection, string whereSection, string orderBySection)
        {
            this.Src = src;
            this._typeIsAttributed = typeIsAttributed;
            if (typeof(T).IsGenericType &&
                typeof(T).GetGenericTypeDefinition() == typeof(JoinedRecord<,>)) {
                _fieldsList = null;
                _fieldsNames = null;
            } else if (typeIsAttributed) {
                DataSource.getTypeRelationFields<T>(out _fieldsList, out _fieldsNames);
            } else {
                DataSource.getTypeRelationFieldsNoAttributes<T>(out _fieldsList, out _fieldsNames);
            }

            this.FromSection = fromSection;
            this.WhereSection = whereSection;
            this.OrderBySection = orderBySection;
        }

        protected virtual string FromSection { get; }

        protected virtual string SelectFields => string.Join(",", _fieldsNames.ToArray());

        #region IEnumerable<T> Members

        public IEnumerator<T> GetEnumerator()
        {
            var sql = GetDefaultSql(null);
            var r = Src.ExecuteReader(sql);
            return new DataReaderNonTableEnumerator<T>(Src, _fieldsList, r);
        }

        #endregion

        #region IEnumerable Members

        IEnumerator IEnumerable.GetEnumerator()
        {
            var sql = GetDefaultSql(null);
            var r = Src.ExecuteReader(sql);
            return new DataReaderNonTableEnumerator<T>(Src, _fieldsList, r);
        }

        #endregion

        public IEnumerable<GroupResult> Group(string byField, string extraField, SqlFunction f, string functionField,
            string havingSection)
        {
            var sb = new StringBuilder();
            sb.Append("SELECT " + byField);
            if (extraField + "" != "")
                sb.Append(", " + extraField);
            sb.Append(", " + f.ToString().ToUpper() + "(" + functionField + ")");
            sb.Append(" FROM " + FromSection);
            sb.Append(GetWhere(null));
            sb.Append(" GROUP BY " + byField);
            if (extraField + "" != "")
                sb.Append(", " + extraField);
            if (havingSection + "" != "")
                sb.Append(" HAVING " + havingSection);
            sb.Append(";");
            var sql = sb.ToString();
            var resultRecordFields = new List<string> {"groupedByField", "functionField"};
            if (extraField + "" != "")
                resultRecordFields.Insert(1, "extraField");
            var groupFields = (from fn in resultRecordFields
                select typeof(GroupResult).GetField(fn)).ToList();
            var result =
                new QueryNonTableEnumerable<GroupResult>
                    (Src, groupFields, sql);
            return result;
        }

        protected virtual string GetWhereSection(string moreCriteria)
        {
            if (WhereSection + "" == "" && moreCriteria + "" == "")
                return "";
            if (WhereSection + "" == "")
                return moreCriteria;
            if (moreCriteria + "" == "")
                return WhereSection;
            return "(" + WhereSection + ") and (" + moreCriteria + ")";
        }

        protected virtual string GetWhere(string moreCriteria)
        {
            var ws = GetWhereSection(moreCriteria);
            if (ws + "" == "")
                return "";
            return " where " + ws + " ";
        }

        #region IQuerySet<T> Members

        public int Count => Src.ExecuteScalarSql("select count(*) from " + FromSection + GetWhere(null) + ";");

        public decimal Function(SqlFunction f, string overField)
        {
            var sb = new StringBuilder();
            sb.Append("select " + f.ToString().ToUpper() + "(" + overField + ") from " + FromSection);
            sb.Append(GetWhere(null) + ";");

            return Convert.ToDecimal(Src.ExecuteScalarSqlToObject(sb.ToString()));
        }

        public decimal Sum(string overField) => Function(SqlFunction.Sum, overField);

        public List<T> ToList()
        {
            // ReSharper disable once UnusedVariable
            var sql = GetDefaultSql(null);
            IEnumerable<T> enThis = this;
            return new List<T>(enThis);
        }

        protected virtual string GetDefaultSql(string criteria)
        {
            var sb = new StringBuilder();
            sb.Append("select " + SelectFields);
            sb.Append(" from " + FromSection);
            sb.Append(GetWhere(criteria));
            if (OrderBySection + "" != "")
                sb.Append(" order by " + OrderBySection);
            sb.Append(";");
            var sql = sb.ToString();
            return sql;
        }

        public T First
        {
            get
            {
                var sb = new StringBuilder();
                sb.Append("select top 1 " + SelectFields);
                sb.Append(" from " + FromSection);
                sb.Append(GetWhere(null));
                if (OrderBySection + "" != "")
                    sb.Append(" order by " + OrderBySection);
                sb.Append(";");

                var tmpEn = new QueryNonTableEnumerable<T>(Src, _fieldsList, sb.ToString());
                var tmpList = new List<T>(tmpEn);
                if (tmpList.Count == 0)
                    return null;
                return tmpList[0];
            }
        }

        public IEnumerable<T> Query(string criteria)
        {
            var sql = GetDefaultSql(criteria);
            return new QueryNonTableEnumerable<T>(Src, _fieldsList, sql);
        }

        public IEnumerable<T> Like(string indexValuePattern)
        {
            if (_typeIsAttributed) {
                var tmpTable = new ReflectedTable<T>(Src);
                return Query(tmpTable.GetLikeCriteria(indexValuePattern));
            }

            string likeField = null;
            for (var i = 0; i < _fieldsList.Count; i++)
                if (_fieldsList[i].FieldType == typeof(string)) {
                    likeField = _fieldsNames[i];
                    break;
                }

            return Query(ReflectedTable<T>.GetLikeStatement(likeField, indexValuePattern));
        }

        public IEnumerable<T> QueryByField(string fieldName, object v) =>
            Query(_fieldsNames + " = " + DataSource.ValueToSql(v));

        public int QueryCount(string criteria)
        {
            var sb = new StringBuilder();
            sb.Append("select count(*)");
            sb.Append(" from " + FromSection);
            sb.Append(GetWhere(criteria));
            sb.Append(";");
            return Src.ExecuteScalarSql(sb.ToString());
        }

        public virtual IQuerySet<T> Sort(string fieldsList) =>
            new QueryNonTableSet<T>(Src, _typeIsAttributed, FromSection, WhereSection, fieldsList);

        public virtual IQuerySet<T> Subset(string moreCriteria) =>
            new QueryNonTableSet<T>(Src, _typeIsAttributed, FromSection,
                GetWhereSection(moreCriteria), OrderBySection);

        #endregion
    }
}