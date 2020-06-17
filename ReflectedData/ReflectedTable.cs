
#region using

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
// ReSharper disable UnusedMember.Global

#endregion

// TODO: update framework to address SQL syntax differences between different data sources
// specifically for dates and "top" clauses, see: http://www.w3schools.com/Sql/sql_top.asp
namespace ReflectedData
{
    /// <summary>
    ///     Access a table defined by fields in T. Assign attribute DataField to fields matched in data.
    ///     Default table name is class's name concat with the letter 's'. For example,
    ///     "Trainee" class is assumed to belong to table name "Trainees".
    ///     To rename the default table-name, add a public static property called
    ///     TableName which returns a string.
    /// </summary>
    /// <remarks>A table needs id or indexed field, both are not necessary though</remarks>
    /// <typeparam name="T">T is a row class in the table</typeparam>
    public class ReflectedTable<T> where T : class, new()
    {
        readonly List<FieldInfo> _fields = new List<FieldInfo>();
        readonly List<string> _fieldsNames = new List<string>();
        internal bool? HasJoinSets;
        public string IDField; //"ID";
        // ReSharper disable once InconsistentNaming
        public FieldInfo idFieldInfo;
        public string IndexField;
        readonly FieldInfo _indexFieldInfo;
        public string TableName;

        public ReflectedTable(DataSource file)
        {
            MyDataSource = file;
            var ttype = typeof(T);
            TableName = DataSource.GetLineTableName(ttype);
            var classAttributes = ttype.GetCustomAttributes(typeof(DataRecordAttribute), true);
            var allFields = false;
            if (classAttributes.Length > 0) {
                var dataRecordAttr = (DataRecordAttribute) classAttributes[0];
                if (dataRecordAttr.TableName != null)
                    TableName = dataRecordAttr.TableName;
                allFields = dataRecordAttr.AllFields;
                IDField = dataRecordAttr.IDField;
                IndexField = dataRecordAttr.IndexField;
            }

            foreach (var finfo in ttype.GetFields(BindingFlags.Public | BindingFlags.Instance |
                                                  BindingFlags.FlattenHierarchy)) {
                var fieldName = finfo.Name;
                var attributes = finfo.GetCustomAttributes(typeof(DataFieldAttribute), true);
                if (attributes.Length > 0) {
                    var dataFieldAttr = (DataFieldAttribute) attributes[0];
                    if (dataFieldAttr.Ignore)
                        continue;
                    if (dataFieldAttr.Rename != null)
                        fieldName = TableName + ".[" + dataFieldAttr.Rename + "]";
                    if (dataFieldAttr.IsID) {
                        IDField = fieldName;
                        idFieldInfo = finfo;
                        continue;
                    }

                    if (dataFieldAttr.IsIndex) {
                        IndexField = fieldName;
                        _indexFieldInfo = finfo;
                    }
                } else {
                    if (!allFields)
                        continue;
                    if (String.Compare(IDField + "", fieldName, StringComparison.OrdinalIgnoreCase) == 0)
                        idFieldInfo = finfo;
                    if (String.Compare(IndexField + "", fieldName, StringComparison.OrdinalIgnoreCase) == 0)
                        _indexFieldInfo = finfo;
                }

                if (fieldName == IDField)
                    continue;
                _fields.Add(finfo);
                _fieldsNames.Add(fieldName);
            }
        }

        public int Count
        {
            get
            {
                var useField = (IDField ?? IndexField) ?? _fieldsNames[0];
                return MyDataSource.ExecuteScalarSql("select count(" + useField + ") from " + TableName);
            }
        }

        bool IsAccessDb =>
            MyDataSource is DataFileSource source &&
            (source.FileType == DataFileType.Access2007 ||
             source.FileType == DataFileType.AccessMdb);

        /// <summary>
        ///     to update the index value of the index field, use the old value on the table[indexValue]
        ///     and the new value in the instance being set.
        /// </summary>
        /// <param name="indexValue"></param>
        /// <returns></returns>
        public T this[string indexValue]
        {
            get
            {
                var sqlBuilder = new StringBuilder();
                sqlBuilder.Append("SELECT ");
                sqlBuilder.Append(string.Join(",", _fieldsNames.ToArray()));
                if (IDField != null)
                    sqlBuilder.Append(", " + IDField);
                sqlBuilder.Append(" FROM " + TableName + " WHERE " + IndexField + " = " +
                                  DataSource.ValueToSql(indexValue) + ";");
                if (IDField == null) {
                    var result = getLine(sqlBuilder.ToString());
                    return result;
                }

                return getLineExtraFieldIsID(sqlBuilder.ToString());
            }
            set
            {
                var values = new object[_fields.Count];
                for (var i = 0; i < _fields.Count; i++)
                    values[i] = _fields[i].GetValue(value);
                // ReSharper disable once NotAccessedVariable
                var idValue = -1;
                if (IDField != null)
                    // ReSharper disable once RedundantAssignment
                    idValue = (int) idFieldInfo.GetValue(value);

                if (value is ReflectedTableLine rtLine && (rtLine.DataConnected)) {
                    MyDataSource.Update(TableName, IndexField, indexValue, _fieldsNames.ToArray(), values);
                    return;
                }

                var newID = MyDataSource.Insert(TableName, _fieldsNames.ToArray(), values, IDField != null);
                if (newID >= 1)
                    idFieldInfo.SetValue(value, newID);
            }
        }

        public DataSource MyDataSource { get; }

        internal FieldInfo GetFieldInfoFromTableField(string tableFieldName)
        {
            for (var i = 0; i < _fieldsNames.Count; i++) {
                if (String.Compare(_fieldsNames[i], tableFieldName, StringComparison.OrdinalIgnoreCase) == 0)
                    return _fields[i];
                int indexOfDot;
                if ((indexOfDot = _fieldsNames[i].IndexOf('.')) < 0)
                    continue;
                var testStr = _fieldsNames[i].Substring(indexOfDot + 1).Trim('[', ']');
                if (String.Compare(testStr, tableFieldName, StringComparison.OrdinalIgnoreCase) == 0)
                    return _fields[i];
            }

            return null;
        }

        internal static bool IsNullableType(Type theType) =>
            theType.IsGenericType && theType.GetGenericTypeDefinition() == typeof(Nullable<>);

        internal string FieldInfoToSqlType(FieldInfo fi)
        {
            string constraints = null;
            if (idFieldInfo == fi)
                constraints = MyDataSource.GetConstraint_Identity();
            var attr = fi.GetCustomAttributes(typeof(DataFieldAttribute), true);
            if (attr.Length > 0) {
                if (attr[0] is DataFieldAttribute df)
                    if (df.SqlDataType + "" != "")
                        return df.SqlDataType + constraints;
            }

            var fieldType = fi.FieldType;
            if (IsNullableType(fieldType))
                fieldType = fieldType.GetGenericArguments()[0];
            else if (fieldType != typeof(string) && constraints == null)
                constraints = MyDataSource.GetConstraint_NotNull();

            // a small subset of available types
            return MyDataSource.DotNetType_to_dataType(fieldType) + constraints;
        }

        /// <summary>
        ///     call create table command on data source
        /// </summary>
        public void CreateDataTable()
        {
            var sb = new StringBuilder();
            sb.Append("CREATE TABLE ");
            sb.Append(TableName);
            sb.Append("(");
            for (var i = 0; i < _fields.Count; i++) {
                if (i > 0)
                    sb.Append(", ");
                var fn = _fieldsNames[i];
                int fnSepIdx = fn.IndexOf(".[", StringComparison.Ordinal);
                if (MyDataSource is DataFileSource dfSrc && dfSrc.FileType != DataFileType.Access2007 && dfSrc.FileType != DataFileType.AccessMdb && fnSepIdx>=0) {
                    fn = fn.Substring(fnSepIdx + 2, fn.Length - fnSepIdx - 3);
                }
                sb.Append(fn);
                sb.Append(" ");
                sb.Append(FieldInfoToSqlType(_fields[i]));
            }

            if (idFieldInfo != null && !_fields.Contains(idFieldInfo)) {
                sb.Append(", ");
                var fn = IDField;
                int fnSepIdx = fn.IndexOf(".[", StringComparison.Ordinal);
                if (MyDataSource is DataFileSource dfSrc && dfSrc.FileType != DataFileType.Access2007 && dfSrc.FileType != DataFileType.AccessMdb && fnSepIdx>=0) {
                    fn = fn.Substring(fnSepIdx + 2, fn.Length - fnSepIdx - 3);
                }
                sb.Append(fn);
                sb.Append(" ");
                sb.Append(FieldInfoToSqlType(idFieldInfo));
            }

            sb.Append(");");
            
            MyDataSource.ExecuteSql(sb.ToString());
            if (_indexFieldInfo == null)
                return;
            if (MyDataSource is DataFileSource dfSrc1 && dfSrc1.FileType != DataFileType.Access2007 && dfSrc1.FileType != DataFileType.AccessMdb)
                return;
            sb = new StringBuilder();
            sb.Append("CREATE INDEX ");
            var idxFld = IndexField;
            int idxFldSepIdx = idxFld.IndexOf(".[", StringComparison.Ordinal);
            if (MyDataSource is DataFileSource dfSrc2 && dfSrc2.FileType != DataFileType.Access2007 && dfSrc2.FileType != DataFileType.AccessMdb && idxFldSepIdx>=0) {
                idxFld = idxFld.Substring(idxFldSepIdx + 2, idxFld.Length - idxFldSepIdx - 3);
            }
            sb.Append(" index_" + idxFld);
            sb.Append(" ON " + TableName);
            sb.Append("(" + idxFld + ");");
            MyDataSource.ExecuteSql(sb.ToString());
        }

        public void DropDataTable()
        {
            MyDataSource.ExecuteSql("DROP TABLE " + TableName + ";");
        }

        internal static object ReadColumn(IDataReader r, int column, Type fieldType)
        {
            #region boring mechanics of comparing types

            if (r.IsDBNull(column))
                return null;
            if (IsNullableType(fieldType))
                fieldType = fieldType.GetGenericArguments()[0];
            try {
                if (fieldType == typeof(string))
                    return r.GetString(column);
                if (fieldType == typeof(int))
                    return r.GetInt32(column);
                if (fieldType == typeof(float))
                    return r.GetFloat(column);
                if (fieldType == typeof(bool))
                    return r.GetBoolean(column);
                if (fieldType == typeof(DateTime))
                    return r.GetDateTime(column);
                if (fieldType == typeof(decimal))
                    return r.GetDecimal(column);
                if (fieldType == typeof(byte))
                    return r.GetByte(column);
            } catch {
                //Type columnType = r.GetFieldType(column);
                var v = r.GetValue(column);
                if (fieldType == typeof(string))
                    return v + "";
                if (fieldType == typeof(int))
                    return Convert.ToInt32(v);
                if (fieldType == typeof(float))
                    return Convert.ToSingle(v);
                if (fieldType == typeof(bool)) {
                    var vStr = v + "";
                    if (vStr == "1" || vStr == "True" || vStr == "T" || vStr == "Y" || vStr == "Yes")
                        return true;
                    if (vStr == "0" || vStr == "False" || vStr == "F" || vStr == "N" || vStr == "No")
                        return false;

                    return Convert.ToBoolean(v);
                }

                if (fieldType == typeof(DateTime))
                    return Convert.ToDateTime(v);
                if (fieldType == typeof(decimal))
                    return Convert.ToDecimal(v);
                if (fieldType == typeof(byte))
                    return Convert.ToByte(v);
            }

            return r.GetValue(column);

            #endregion
        }

        static void readerToField(IDataReader r, int column, FieldInfo fi, T line)
        {
            var v = ReadColumn(r, column, fi.FieldType);
            if (v == null && !IsNullableType(fi.FieldType))
                return;
            fi.SetValue(line, v);
        }

        T getLine(string sql)
        {
            var r = MyDataSource.ExecuteReader(sql);
            if (!r.Read())
                return null;
            var line = new T();
            for (var i = 0; i < _fields.Count; i++)
                readerToField(r, i, _fields[i], line);
            connectLine(line);
            MyDataSource.DisposeReader(r);
            return line;
        }

        T getLineExtraFieldIsID(string sql)
        {
            var r = MyDataSource.ExecuteReader(sql);
            if (!r.Read()) {
                MyDataSource.DisposeReader(r);
                return null;
            }

            var line = new T();
            for (var i = 0; i < _fields.Count; i++)
                readerToField(r, i, _fields[i], line);
            connectLine(line);
            readerToField(r, _fields.Count, idFieldInfo, line);
            MyDataSource.DisposeReader(r);
            return line;
        }

        public T Get(int id)
        {
            var sqlBuilder = new StringBuilder();
            sqlBuilder.Append("SELECT ");
            sqlBuilder.Append(string.Join(",", _fieldsNames.ToArray()));
            sqlBuilder.Append(" FROM " + TableName + " WHERE " + IDField + " = " + id + ";");
            var line = getLine(sqlBuilder.ToString());
            if (line == null)
                return null;
            idFieldInfo.SetValue(line, id);
            return line;
        }

        public T AtDateFirst(DateTime atDate) => AtDateSet(atDate).First;

        public QueryTableSet<T> AtDateSet(DateTime atDate)
        {
            var dateFieldIndex = findDateFieldIndex();

            var fieldName = _fieldsNames[dateFieldIndex];
            var atDateStr = "'" + atDate.ToString("yyyy-MM-dd") + "'";
            // ReSharper disable once StringLiteralTypo
            if (IsAccessDb) atDateStr = "DATEVALUE(" + atDateStr + ")";

            return new QueryTableSet<T>(fieldName + "=" + atDateStr, null, this);
        }

        public DateTime? GetFirstDate()
        {
            var dateFieldIndex = findDateFieldIndex();
            var fieldName = _fieldsNames[dateFieldIndex];
            var result =
                MyDataSource.ExecuteScalarSqlToObject("SELECT TOP 1 " + fieldName + " FROM " + TableName +
                                                      " ORDER BY " + fieldName);
            return result as DateTime?;
        }

        public DateTime? GetLastDate()
        {
            var dateFieldIndex = findDateFieldIndex();
            var fieldName = _fieldsNames[dateFieldIndex];
            var result =
                MyDataSource.ExecuteScalarSqlToObject("SELECT TOP 1 " + fieldName + " FROM " + TableName +
                                                      " ORDER BY " + fieldName + " DESC");
            return result as DateTime?;
        }

        public QueryTableSet<T> DateRangeSet(DateTime fromDate, DateTime toDate)
        {
            var queryWhere = DateRangeToCriteria(fromDate, toDate);

            return new QueryTableSet<T>(queryWhere, null, this);
        }

        /// <summary>
        ///     creates the text of the where clause of an sql statement for a date range relevant to this table's
        ///     date field and uses the function DATEVALUE if source is access file
        /// </summary>
        /// <param name="fromDate"></param>
        /// <param name="toDate"></param>
        /// <returns></returns>
        public string DateRangeToCriteria(DateTime fromDate, DateTime toDate)
        {
            var dateFieldIndex = findDateFieldIndex();

            var fieldName = _fieldsNames[dateFieldIndex];
            var fromDateStr = "'" + fromDate.ToString("yyyy-MM-dd") + "'";
            var toDateStr = "'" + toDate.ToString("yyyy-MM-dd") + "'";
            if (IsAccessDb) {
                // ReSharper disable StringLiteralTypo
                fromDateStr = "DATEVALUE(" + fromDateStr + ")";
                toDateStr = "DATEVALUE(" + toDateStr + ")";
                // ReSharper restore StringLiteralTypo
            }

            var queryWhere = fieldName + " between " + fromDateStr + " and " + toDateStr;
            return queryWhere;
        }

        int findDateFieldIndex()
        {
            for (var i = 0; i < _fields.Count; i++)
                if (_fields[i].FieldType == typeof(DateTime) ||
                    _fields[i].FieldType == typeof(DateTime?))
                    return i;
            throw new Exception("No date field found");
        }

        public bool DeleteByDateRange(DateTime fromDate, DateTime toDate) =>
            MyDataSource.Delete(TableName, DateRangeToCriteria(fromDate, toDate));

        /// <summary>
        ///     deletes all records in a table - not a common thing to do with a relational database...
        /// </summary>
        /// <returns></returns>
        public void Clear()
        {
            MyDataSource.ExecuteSql("delete from " + TableName + ";");
        }

        public void Update(T line)
        {
            var rtLine = line as ReflectedTableLine;
            if (rtLine != null && rtLine.DataConnected && rtLine.IsDeleted)
                throw new InvalidOperationException("Cannot update a deleted record");
            var values = new object[_fields.Count];
            for (var i = 0; i < _fields.Count; i++)
                values[i] = _fields[i].GetValue(line);
            var byFieldName = IDField ?? IndexField;
            var byLineFieldInfo = IDField == null ? _indexFieldInfo : idFieldInfo;
            var byFieldValue = byLineFieldInfo.GetValue(line);
            if (IDField == null && rtLine != null)
                byFieldValue = rtLine.IndexCopy;
            MyDataSource.Update(TableName, byFieldName, byFieldValue, _fieldsNames.ToArray(), values);
            connectLine(line);
        }

        public void Insert(T line)
        {
            var values = new object[_fields.Count];
            for (var i = 0; i < _fields.Count; i++)
                values[i] = _fields[i].GetValue(line);
            var newID = MyDataSource.Insert(TableName, _fieldsNames.ToArray(), values, IDField != null);
            if (IDField != null)
                idFieldInfo.SetValue(line, newID);
            connectLine(line);
        }

        /// <summary>
        ///     unlike insert, this does not retrieve id field - and does not connect the instance - so update
        ///     and delete would not be valid after this operation.
        ///     ConnectionSource is not supported for this operation
        /// </summary>
        /// <param name="lines"></param>
        public void InserBulk(T[] lines)
        {
            var values = new object[lines.Length, _fields.Count];
            for (var lineIndex = 0; lineIndex < lines.Length; lineIndex++)
            for (var i = 0; i < _fields.Count; i++)
                values[lineIndex, i] = _fields[i].GetValue(lines[lineIndex]);
            var fieldTypes = from f in _fields
                select f.FieldType;
            MyDataSource.InsertBulk(TableName, _fieldsNames.ToArray(), fieldTypes.ToArray(), values);
        }

        public string GetInsertSql(T line)
        {
            var values = new object[_fields.Count];
            for (var i = 0; i < _fields.Count; i++)
                values[i] = _fields[i].GetValue(line);
            return DataSource.GetInsertSql(TableName, _fieldsNames.ToArray(), values);
        }

        public bool Delete(T line)
        {
            bool result;
            var rtlLine = line as ReflectedTableLine;
            if (rtlLine != null && rtlLine.IsDeleted)
                return false;
            if (IDField != null) {
                var id = (int) idFieldInfo.GetValue(line);
                result = Delete(id);
                idFieldInfo.SetValue(line, -1);
            } else {
                var indexValue = (string) _indexFieldInfo.GetValue(line);
                result = Delete(indexValue);
            }

            if (rtlLine == null)
                return result;
            rtlLine.IsDeleted = true;
            return result;
        }

        public bool Delete(int id) => MyDataSource.Delete(TableName, IDField, id);

        public bool Delete(string indexValue) => MyDataSource.Delete(TableName, IndexField, indexValue);

        public T SelectFirst(string criteria)
        {
            var result = SelectList(1, criteria);
            if (result.Count == 0)
                return null;
            return result[0];
        }

        public List<T> SelectList(string criteria) => SelectList(-1, criteria, null);

        public List<T> SelectList(int topCount, string criteria) => SelectList(topCount, criteria, null);

        public List<T> SelectList(int topCount, string criteria, string orderBy)
        {
            var sql = BuildSelectSql(topCount, criteria, orderBy);
            var r = MyDataSource.ExecuteReader(sql);
            var result = new List<T>();
            while (r.Read()) {
                var line = readerToLine(r);
                result.Add(line);
            }

            MyDataSource.DisposeReader(r);
            return result;
        }

        public string BuildSelectSql(int topCount, string criteria) => BuildSelectSql(topCount, criteria, null);

        public string BuildSelectSql(int topCount, string criteria, string orderBy)
        {
            var sqlBuilder = new StringBuilder();
            sqlBuilder.Append("SELECT ");
            if (topCount > 0)
                sqlBuilder.Append("TOP " + topCount + " ");
            sqlBuilder.Append(BuildSqlFieldsList());
            var buildWhere = criteria + "" == "" ? "" : " WHERE " + criteria;
            sqlBuilder.Append(" FROM " + TableName + buildWhere);
            if (orderBy + "" != "")
                sqlBuilder.Append(" ORDER BY " + orderBy);
            sqlBuilder.Append(";");
            return sqlBuilder.ToString();
        }

        /// <summary>
        ///     using the reflected list of type fields, build list of table fields in proper order
        ///     last field is ID field if one exists
        /// </summary>
        /// <returns></returns>
        public string BuildSqlFieldsList()
        {
            if (IDField != null)
                return string.Join(",", _fieldsNames.ToArray()) + ", " + IDField;
            return string.Join(",", _fieldsNames.ToArray());
        }

        public string BuildSqlFieldsListWithTable()
        {
            var resultNames = new List<string>();
            foreach (var fn in _fieldsNames)
                if (fn.IndexOf('.') < 0)
                    resultNames.Add(TableName + "." + fn);
                else
                    resultNames.Add(fn);
            if (IDField != null) {
                if (IDField.IndexOf('.') < 0)
                    resultNames.Add(TableName + "." + IDField);
                else
                    resultNames.Add(IDField);
            }

            return string.Join(",", resultNames.ToArray());
        }

        public IEnumerable<T> Select(int topCount, string criteria, string orderBy)
        {
            var sql = BuildSelectSql(topCount, criteria, orderBy);
            return new QueryTableEnumerable<T>(this, sql);
        }

        public IEnumerable<T> Select(int topCount, string criteria)
        {
            var sql = BuildSelectSql(topCount, criteria);
            return new QueryTableEnumerable<T>(this, sql);
        }

        public IEnumerable<T> Select(string criteria) => Select(-1, criteria);

        public IEnumerable<JoinedRecord<T, TC>> JoinEn<TC>(int topCount, string onChildField, string criteria,
            string orderBy)
            where TC : class, new()
        {
            var cTable = MyDataSource.Table<TC>();
            var fieldsStr = BuildSqlFieldsListWithTable() + ", " + cTable.BuildSqlFieldsListWithTable();

            var sqlBuilder = new StringBuilder();
            sqlBuilder.Append("SELECT ");
            if (topCount > 0)
                sqlBuilder.Append("TOP " + topCount + " ");
            sqlBuilder.Append(fieldsStr);
            var buildWhere = criteria + "" == "" ? "" : " WHERE " + criteria;
            // Customers LEFT JOIN Orders ON Customers.ID = Orders.customerID
            sqlBuilder.Append(" FROM " + TableName + " LEFT JOIN " + cTable.TableName + " ON " +
                              TableName + "." + IDField + " = " +
                              cTable.TableName + "." + onChildField);

            sqlBuilder.Append(buildWhere);
            if (orderBy + "" != "")
                sqlBuilder.Append(" ORDER BY " + orderBy);
            sqlBuilder.Append(";");

            return new QueryNonTableEnumerable<JoinedRecord<T, TC>>(MyDataSource, null, sqlBuilder.ToString());
        }

        // ReSharper disable once InconsistentNaming
        internal T readerToLine(IDataReader r) => readerToLine(r, 0);

        // ReSharper disable once InconsistentNaming
        internal T readerToLine(IDataReader r, int startAtIndex)
        {
            var line = new T();
            for (var i = 0; i < _fields.Count; i++)
                readerToField(r, i + startAtIndex, _fields[i], line);
            if (IDField != null)
                readerToField(r, _fields.Count + startAtIndex, idFieldInfo, line);
            connectLine(line);
            return line;
        }

        void connectLine(T line)
        {
            var rtLine = line as ReflectedTableLine;
            if (rtLine == null)
                return;
            if (rtLine.DataConnected)
                return;
            rtLine.Source = MyDataSource;
            rtLine.AtTable = this;
            rtLine.DataConnected = true;
            rtLine.IsDeleted = false;
            if (idFieldInfo == null && _indexFieldInfo != null)
                rtLine.IndexCopy = (string) _indexFieldInfo.GetValue(line);
            if (HasJoinSets.HasValue && !HasJoinSets.Value)
                return;

            HasJoinSets = rtLine.InstantiateJoinSets();
        }

        public List<T> LikeList(string indexValuePattern) => SelectList(GetLikeCriteria(indexValuePattern));

        public IEnumerable<T> Like(string indexValuePattern) => Like(indexValuePattern, null);

        public IEnumerable<T> Like(string indexValuePattern, string moreCriteria)
        {
            if (moreCriteria + "" == "")
                return Select(GetLikeCriteria(indexValuePattern));
            return Select(GetLikeCriteria(indexValuePattern) + " and " + moreCriteria);
        }

        public static string GetLikeStatement(string likeField, string indexValuePattern)
        {
            if (indexValuePattern.IndexOf('%') < 0)
                indexValuePattern += "%";
            return likeField + " LIKE " + DataSource.ValueToSql(indexValuePattern);
        }

        public string GetLikeCriteria(string indexValuePattern)
        {
            var likeField = IndexField;
            if (likeField == null)
                for (var i = 0; i < _fields.Count; i++)
                    if (_fields[i].FieldType == typeof(string)) {
                        likeField = _fieldsNames[i];
                        break;
                    }

            return GetLikeStatement(likeField, indexValuePattern);
        }

        public int CountQuery(string criteria)
        {
            var useField = (IDField ?? IndexField) ?? _fieldsNames[0];
            return MyDataSource.ExecuteScalarSql("select count(" + useField + ") from " + TableName + " where " +
                                                 criteria);
        }

        /// <summary>
        ///     get all records
        /// </summary>
        /// <returns></returns>
        public List<T> ToList() => SelectList("");

        public IEnumerable<T> ToEnumerable() => Select("");

        public static List<T> ToList(DataSource f)
        {
            var me = f.Table<T>();
            return me.ToList();
        }

        public static List<T> ToListFillJoins(DataSource f)
        {
            var me = f.Table<T>();
            var result = me.ToList();
            me.FillJoins(result);
            return result;
        }

        public List<T> ByField(string fieldName, object fieldValue) =>
            SelectList(fieldName + "=" + DataSource.ValueToSql(fieldValue));

        public JoinSet<T> ByFieldSet(string fieldName, object fieldValue) =>
            new JoinSet<T>(fieldName, (int) fieldValue, this);

        public List<T> ByField(int topCount, string fieldName, object fieldValue) =>
            SelectList(topCount, fieldName + "=" + DataSource.ValueToSql(fieldValue));

        public T ByFieldFirst(string fieldName, object fieldValue)
        {
            var result = SelectList(1, fieldName + "=" + DataSource.ValueToSql(fieldValue));
            if (result.Count == 0)
                return null;
            return result[0];
        }

        public void FillJoins(IEnumerable<T> items)
        {
            if (!typeof(ReflectedTableLine).IsAssignableFrom(typeof(T)))
                return;
            foreach (object line in items)
                ((ReflectedTableLine) line).FillJoins();
        }

        public void GetInstanceValues(T item, out FieldInfo[] fieldsInfo, out string[] fieldsNames,
            out object[] fieldsValues)
        {
            fieldsInfo = _fields.ToArray();
            fieldsNames = this._fieldsNames.ToArray();
            fieldsValues = new object[fieldsInfo.Length];
            for (var i = 0; i < fieldsInfo.Length; i++)
                fieldsValues[i] = fieldsInfo[i].GetValue(item);
        }

        public void GetFieldsArraysWithId(out FieldInfo[] fieldsInfo, out string[] fieldNames, out bool hasId)
        {
            var fieldsCopy = new List<FieldInfo>(_fields);
            var fieldsNamesCopy = new List<string>(_fieldsNames);
            hasId = IDField != null;
            if (hasId) {
                fieldsCopy.Add(idFieldInfo);
                fieldsNamesCopy.Add(IDField);
            }

            fieldsInfo = fieldsCopy.ToArray();
            fieldNames = fieldsNamesCopy.ToArray();
        }
    }
}