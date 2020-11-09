
#region using

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;

// ReSharper disable UnusedMember.Global

#endregion

// TODO: update framework to address SQL syntax differences between different data sources
// specifically for dates and "top" clauses, see: http://www.w3schools.com/Sql/sql_top.asp
// ReSharper disable once CheckNamespace
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
        readonly List<FieldInfo> _fieldInfos = new List<FieldInfo>(); // instance reflection fields, indexes match _dbFieldNames
        readonly List<string> _dbFieldNames = new List<string>(); // database names of fields, indexes match _fieldInfos
        internal bool? HasJoinSets;
        public string IDField; //"ID";
        // ReSharper disable once InconsistentNaming
        public FieldInfo idFieldInfo;
        public string IndexField;
        readonly FieldInfo _indexFieldInfo;
        public string TableName;
        //If IDField is not null, allows specifying that it is not auto incrementing and thus must be specified on an insert operation
        public bool AutoIncrement = true;
        static bool SmartUpdate => DataSource.SmartUpdates;

        public FieldInfo GetColumnFieldInfo(int i)
        {
            if (i == 0) return idFieldInfo;
            return _fieldInfos[i - 1];
        }

        public string GetColumnName(int i)
        {
            if (i == 0) return IDField;
            return _dbFieldNames[i];
        }


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
                AutoIncrement = dataRecordAttr.AutoIncrement;
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
                        AutoIncrement = dataFieldAttr.IsAutoIncrement;
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
                _fieldInfos.Add(finfo);
                _dbFieldNames.Add(fieldName);
            }
        }
        /// <summary>
        /// Get the names of the columns as defined in the database
        /// </summary>
        /// <returns></returns>
        public string[] GetDbFieldNames()
        {
            string sql = "select Top 1 * from " + TableName;
            var reader = MyDataSource.ExecuteReader(sql);
            var schemaTable = reader.GetSchemaTable();
            MyDataSource.DisposeReader(reader);
            return (from dr in schemaTable?.Rows.Cast<DataRow>()
                select dr["ColumnName"].ToString()).ToArray();
        }

        public TField[] GetFieldDistinctValues<TField>(string fieldName, bool sort = false)
        {
            var fi = typeof(T).GetField(fieldName);
            if (fi == null) throw new Exception("FieldName doesn't match any public field in type");
            int fiIndex = _fieldInfos.IndexOf(fi);
            var dbFn = _dbFieldNames[fiIndex];
            var sql = $"select distinct {dbFn} from {TableName}" + (sort ? $" order by {dbFn} asc;" : "");
            var r = MyDataSource.ExecuteReader(sql);
            var result = new List<TField>();
            while (r.Read()) {
                var vO = r[0];
                result.Add((TField) vO);
            }
            MyDataSource.DisposeReader(r);
            return result.ToArray();
        }

        public int[] GetIDs(bool sortByID = false)
        {
            if (string.IsNullOrEmpty(IDField)) return null;

            var sql = "select " + IDField + " from " + TableName + (sortByID ? " order by " + IDField + " asc;" : "");
            var r = MyDataSource.ExecuteReader(sql);
            var result = new List<int>();
            while (r.Read()) {
                var id = r.GetInt32(0);
                result.Add(id);
            }

            MyDataSource.DisposeReader(r);
            return result.ToArray();
        }
        /// <summary>
        /// Add missing fields to database that are found in record class.
        /// </summary><returns>true if changed, false if no need to change, throw exceptions on errors</returns>
        public bool VerifyOrCreateClassFieldsInDbTable(bool createTableIfMissing = true)
        {
            if (createTableIfMissing && !MyDataSource.GetDataTableNames().Contains(TableName)) {
                CreateDataTable();
                return true;
            }
            // field name exceptions, index matches location in fieldInfos
            var missingDbFieldNames = _dbFieldNames.Except(GetDbFieldNames()).ToArray();
            if (missingDbFieldNames.Length == 0) return false;
            try {
                var sb = new StringBuilder();
                bool firstField = true;
                foreach (var dbFieldName in missingDbFieldNames) {
                    if (!firstField)
                        sb.Append(",");
                    
                    int fiIndex = _dbFieldNames.IndexOf(dbFieldName);
                    string fixDbFn = dbFieldName;

                    int fnSepIdx = fixDbFn.IndexOf(".[", StringComparison.Ordinal);
                    if (MyDataSource is DataFileSource dfSrc && dfSrc.FileType != DataFileType.Access2007 &&
                        dfSrc.FileType != DataFileType.AccessMdb && fnSepIdx >= 0)
                        fixDbFn = fixDbFn.Substring(fnSepIdx + 2, fixDbFn.Length - fnSepIdx - 3);
                    sb.Append(fixDbFn);
                    sb.Append(" ");
                    sb.Append(FieldInfoToSqlType(_fieldInfos[fiIndex]));
                    firstField = false;
                }

                MyDataSource.ExecuteSql($"alter table {TableName} add {sb}");
            } catch  {
                //on fail remove all fieldInfos not contained in database.
                foreach (var dbFieldName in missingDbFieldNames) {
                    int fiIndex = _dbFieldNames.IndexOf(dbFieldName);
                    _fieldInfos.RemoveAt(fiIndex);
                    _dbFieldNames.RemoveAt(fiIndex);
                }

                throw;
            }

            return true;
        }
        /// <summary>
        /// Verify field names against database. Combines the ability to add and remove
        /// fields from Database.
        /// </summary><returns>true if modified, false if not needed, throws exception if fail</returns>
        public bool VerifyOrCreateAndRemoveFields(bool createTableIfMissing = true)
        {
            VerifyOrCreateClassFieldsInDbTable(createTableIfMissing);
            var dbAllFieldNames = _dbFieldNames.ToArray().ToList();
            dbAllFieldNames.Add(IDField);
            var extraDbFieldNames = GetDbFieldNames().Except(dbAllFieldNames).ToArray();
            if (extraDbFieldNames.Length == 0) return false;
            var sb = new StringBuilder();
            bool firstField = true;
            foreach (var fieldName in extraDbFieldNames) {
                if (firstField) sb.Append(",");
                sb.Append(fieldName);
                sb.Append(" ");
                firstField = false;
            }

            MyDataSource.ExecuteSql($"alter table {TableName} drop {sb}");

            return true;
        }
        /// <summary>
        /// return the number of lines in the table based on ID-Field, or on 1st field. Does not define "distinct" - so should work even if same values in first field.
        /// </summary>
        public int Count
        {
            get
            {
                var useField = (IDField ?? IndexField) ?? _dbFieldNames[0];
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
                sqlBuilder.Append(string.Join(",", _dbFieldNames.ToArray()));
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
                var values = new object[_fieldInfos.Count];
                for (var i = 0; i < _fieldInfos.Count; i++)
                    values[i] = _fieldInfos[i].GetValue(value);
                // ReSharper disable once NotAccessedVariable
                var idValue = -1;
                if (IDField != null)
                    // ReSharper disable once RedundantAssignment
                    idValue = (int) idFieldInfo.GetValue(value);

                if (value is ReflectedTableLine rtLine && (rtLine.DataConnected)) {
                    MyDataSource.Update(TableName, IndexField, indexValue, _dbFieldNames.ToArray(), values);
                    return;
                }

                var newID = MyDataSource.Insert(TableName, _dbFieldNames.ToArray(), values, IDField != null);
                if (newID >= 1)
                    idFieldInfo.SetValue(value, newID);
            }
        }

        public DataSource MyDataSource { get; }

        internal FieldInfo GetFieldInfoFromTableField(string tableFieldName)
        {
            for (var i = 0; i < _dbFieldNames.Count; i++) {
                if (String.Compare(_dbFieldNames[i], tableFieldName, StringComparison.OrdinalIgnoreCase) == 0)
                    return _fieldInfos[i];
                int indexOfDot;
                if ((indexOfDot = _dbFieldNames[i].IndexOf('.')) < 0)
                    continue;
                var testStr = _dbFieldNames[i].Substring(indexOfDot + 1).Trim('[', ']');
                if (String.Compare(testStr, tableFieldName, StringComparison.OrdinalIgnoreCase) == 0)
                    return _fieldInfos[i];
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
            for (var i = 0; i < _fieldInfos.Count; i++) {
                if (i > 0)
                    sb.Append(", ");
                var fn = _dbFieldNames[i];
                int fnSepIdx = fn.IndexOf(".[", StringComparison.Ordinal);
                if (MyDataSource is DataFileSource dfSrc && dfSrc.FileType != DataFileType.Access2007 && dfSrc.FileType != DataFileType.AccessMdb && fnSepIdx>=0) {
                    fn = fn.Substring(fnSepIdx + 2, fn.Length - fnSepIdx - 3);
                }
                sb.Append(fn);
                sb.Append(" ");
                sb.Append(FieldInfoToSqlType(_fieldInfos[i]));
            }

            if (idFieldInfo != null && !_fieldInfos.Contains(idFieldInfo)) {
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
        static bool eq(string a, string b, bool ignoreCase = true) => 
            string.Equals(a, b, ignoreCase? StringComparison.InvariantCultureIgnoreCase : StringComparison.InvariantCulture);
        public static object ReadColumn(IDataReader r, int column, Type fieldType)
        {
            #region boring mechanics of comparing types
            bool isNullable = IsNullableType(fieldType);
            if (isNullable)
                fieldType = fieldType.GetGenericArguments()[0];
            if (r.IsDBNull(column)) {
                if (!isNullable && fieldType!=typeof(string))
                    return Activator.CreateInstance(fieldType);
                return null;
            }
            var result = r.GetValue(column);
            if (result == null) {
                if (!isNullable && fieldType!=typeof(string))
                    return Activator.CreateInstance(fieldType);
                return null;
            }
            if (result.GetType() == fieldType) return result;
            try {
                switch (fieldType.Name) {
                    case "Int32": return Convert.ToInt32(result);
                    case "String": return result + "";
                    case "Int64": return Convert.ToInt64(result);
                    case "Single": return Convert.ToSingle(result, CultureInfo.InvariantCulture);
                    case "Double": return Convert.ToDouble(result, CultureInfo.InvariantCulture);
                    case "Boolean": 
                        var vStr = result + "";
                        if (vStr == "1" || vStr == "True" || vStr == "T" || vStr == "Y" || vStr == "Yes")
                            return true;
                        if (vStr == "0" || vStr == "False" || vStr == "F" || vStr == "N" || vStr == "No")
                            return false;

                        return Convert.ToBoolean(result);
                    case "Decimal": return Convert.ToDecimal(result, CultureInfo.InvariantCulture);
                    case "DateTime": return Convert.ToDateTime(result); // TODO: debug date time conversions
                    case "Byte": return Convert.ToByte(result);
                }
                // attempt explicit type conversion
                return Convert.ChangeType(result, fieldType, CultureInfo.InvariantCulture);
            } catch {
                Debug.WriteLine("Failed to convert (" + result.GetType().Namespace + ") " + result + " to " + fieldType.Name);
            }
            // revert to default value, TODO: utilize field's default value attribute
            return Activator.CreateInstance(fieldType);

            #endregion
        }

        public static void ReaderToField(IDataReader r, int column, FieldInfo fi, T line)
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
            for (var i = 0; i < _fieldInfos.Count; i++)
                ReaderToField(r, i, _fieldInfos[i], line);
            if (SmartUpdate && line is ReflectedTableLine rtl) {
                rtl.__smartUpdate_LastValues = new Dictionary<string, object>();
                for (int i=0;i<_fieldInfos.Count;i++)
                    rtl.__smartUpdate_LastValues.Add(_fieldInfos[i].Name, ReadColumn(r, i, _fieldInfos[i].FieldType));
            }
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
            for (var i = 0; i < _fieldInfos.Count; i++)
                ReaderToField(r, i, _fieldInfos[i], line);
            connectLine(line);
            ReaderToField(r, _fieldInfos.Count, idFieldInfo, line);
            if (SmartUpdate && line is ReflectedTableLine rtl) {
                rtl.__smartUpdate_LastValues = new Dictionary<string, object>();
                for (int i=0;i<_fieldInfos.Count;i++)
                    rtl.__smartUpdate_LastValues.Add(_fieldInfos[i].Name, ReadColumn(r, i, _fieldInfos[i].FieldType));
                rtl.__smartUpdate_LastValues.Add(idFieldInfo.Name, ReadColumn(r, _fieldInfos.Count, idFieldInfo.FieldType));
            }
            MyDataSource.DisposeReader(r);
            return line;
        }

        public T Get(int id)
        {
            var sqlBuilder = new StringBuilder();
            sqlBuilder.Append("SELECT ");
            sqlBuilder.Append(string.Join(",", _dbFieldNames.ToArray()));
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

            var fieldName = _dbFieldNames[dateFieldIndex];
            var atDateStr = "'" + atDate.ToString("yyyy-MM-dd") + "'";
            // ReSharper disable once StringLiteralTypo
            if (IsAccessDb) atDateStr = "DATEVALUE(" + atDateStr + ")";

            return new QueryTableSet<T>(fieldName + "=" + atDateStr, null, this);
        }

        public DateTime? GetFirstDate()
        {
            var dateFieldIndex = findDateFieldIndex();
            var fieldName = _dbFieldNames[dateFieldIndex];
            var result =
                MyDataSource.ExecuteScalarSqlToObject("SELECT TOP 1 " + fieldName + " FROM " + TableName +
                                                      " ORDER BY " + fieldName);
            return result as DateTime?;
        }

        public DateTime? GetLastDate()
        {
            var dateFieldIndex = findDateFieldIndex();
            var fieldName = _dbFieldNames[dateFieldIndex];
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
        // ReSharper disable once CommentTypo

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

            var fieldName = _dbFieldNames[dateFieldIndex];
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
            for (var i = 0; i < _fieldInfos.Count; i++)
                if (_fieldInfos[i].FieldType == typeof(DateTime) ||
                    _fieldInfos[i].FieldType == typeof(DateTime?))
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
            var values = new List<object>(_fieldInfos.Count);
            var fn = new List<string>(_fieldInfos.Count);
            var fiList = new List<FieldInfo>(_fieldInfos.Count); // field-info list of items being updated
            var rtl = line as ReflectedTableLine;
            for (var i = 0; i < _fieldInfos.Count; i++) {
                var v = _fieldInfos[i].GetValue(line);
                if (SmartUpdate && rtl?.__smartUpdate_LastValues != null) {
                    if (rtl.__smartUpdate_LastValues.ContainsKey(_fieldInfos[i].Name)) {
                        if (Equals(v, rtl.__smartUpdate_LastValues[_fieldInfos[i].Name]))
                            continue;
                    }
                }
                values.Add(v);
                fn.Add(_dbFieldNames[i]);
                fiList.Add(_fieldInfos[i]);
            }

            if (values.Count == 0) return;
            var byFieldName = IDField ?? IndexField;
            var byLineFieldInfo = IDField == null ? _indexFieldInfo : idFieldInfo;
            var byFieldValue = byLineFieldInfo.GetValue(line);
            if (IDField == null && rtLine != null)
                byFieldValue = rtLine.IndexCopy;
            MyDataSource.Update(TableName, byFieldName, byFieldValue, fn.ToArray(), values.ToArray());
            connectLine(line);
            if (SmartUpdate && rtl?.__smartUpdate_LastValues!=null) {
                for (var i = 0; i < fn.Count; i++) {
                    var iFieldName = fiList[i].Name;
                    if (rtl.__smartUpdate_LastValues.ContainsKey(iFieldName))
                        rtl.__smartUpdate_LastValues[iFieldName] = values[i];
                    else 
                        rtl.__smartUpdate_LastValues.Add(iFieldName, values[i]);
                }
            }
        }
        // ReSharper disable once UnusedMember.Local
        static bool canBeNull(Type type) => !type.IsValueType || (Nullable.GetUnderlyingType(type) != null);
        public T Insert(T line)
        {
            //Standard inserts will assign the autoincrement, primary key if specified (via IDField)
            //but if IDField is specified and AutoIncrement is false, use the InsertWith ID method
            if (!AutoIncrement && !string.IsNullOrEmpty(IDField))
                return insertWithID(line);
            var values = new List<object>(_fieldInfos.Count);
            var names = new List<string>();
            for (var i = 0; i < _fieldInfos.Count; i++) {
                var v = _fieldInfos[i].GetValue(line);
                // just avoid smart-insert, set all values to avoid crashes by data-base rules
                //if (SmartUpdate && testDefault(v, _fieldInfos[i].FieldType)) continue;
                values.Add(v);
                names.Add(_dbFieldNames[i]);
            }

            var newID = MyDataSource.Insert(TableName, names.ToArray(), values.ToArray(), !string.IsNullOrEmpty(IDField));
            if (!string.IsNullOrEmpty(IDField))
                idFieldInfo.SetValue(line, newID);

            connectLine(line);
            if (SmartUpdate && line is ReflectedTableLine rtl) {
                if (rtl.__smartUpdate_LastValues == null) rtl.__smartUpdate_LastValues = new Dictionary<string, object>();
                foreach (var fi in _fieldInfos) {
                    if (rtl.__smartUpdate_LastValues.ContainsKey(fi.Name))
                        rtl.__smartUpdate_LastValues[fi.Name] = fi.GetValue(line);
                    else
                        rtl.__smartUpdate_LastValues.Add(fi.Name, fi.GetValue(line));
                }
            }
            
            return line; // allow var line = Insert(new T { ... }) format
        }
        static bool testDefault(object v, Type t = null)
        {
            if (v == null) return true;
            if (t == null) t = v.GetType();
            if (!t.IsPrimitive && !t.IsValueType || v.GetType() != t) return false;
            var def = Activator.CreateInstance(t);
            return Equals(v, def);
        }
        /// <summary>
        ///     Alternative mechanism allowing a line to be inserted when it has a manual record ID
        /// </summary>
        T insertWithID(T line)
        {
            if (AutoIncrement) throw new Exception("Can not insert record with manual ID when set to autoincrement");

            var values = new List<object>(_fieldInfos.Count + 1) {idFieldInfo.GetValue(line)};
            var names = new List<string>() {IDField};

            for (var i = 0; i < _fieldInfos.Count; i++) {
                var v = _fieldInfos[i].GetValue(line);
                if (SmartUpdate && testDefault(v, _fieldInfos[i].FieldType)) continue;
                values.Add(v);
                names.Add(_dbFieldNames[i]);
            }

            MyDataSource.Insert(TableName, names.ToArray(), values.ToArray(), false);

            connectLine(line);
            if (SmartUpdate && line is ReflectedTableLine rtl) {
                if (rtl.__smartUpdate_LastValues == null) rtl.__smartUpdate_LastValues = new Dictionary<string, object>();
                foreach (var fi in _fieldInfos) {
                    rtl.__smartUpdate_LastValues.Add(fi.Name, fi.GetValue(line));
                }
            }
            return line; // allow var line = Insert(new T { ... }) format
        }

        /// <summary>
        ///     unlike insert, this does not retrieve id field - and does not connect the instance - so update
        ///     and delete would not be valid after this operation.
        ///     ConnectionSource is not supported for this operation
        /// </summary>
        /// <param name="lines"></param>
        public void InsertBulk(T[] lines)
        {
            var values = new object[lines.Length, _fieldInfos.Count];
            for (var lineIndex = 0; lineIndex < lines.Length; lineIndex++)
            for (var i = 0; i < _fieldInfos.Count; i++)
                values[lineIndex, i] = _fieldInfos[i].GetValue(lines[lineIndex]);
            var fieldTypes = from f in _fieldInfos
                select f.FieldType;
            MyDataSource.InsertBulk(TableName, _dbFieldNames.ToArray(), fieldTypes.ToArray(), values);
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
                var line = ReaderToLine(r);
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
            var buildWhere = string.IsNullOrWhiteSpace(criteria) ? "" : " WHERE " + criteria;
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
                return string.Join(",", _dbFieldNames.ToArray()) + ", " + IDField;
            return string.Join(",", _dbFieldNames.ToArray());
        }

        public string BuildSqlFieldsListWithTable()
        {
            var resultNames = new List<string>();
            foreach (var fn in _dbFieldNames)
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
        public T ReaderToLine(IDataReader r) => ReaderToLine(r, 0);

        // ReSharper disable once InconsistentNaming
        public T ReaderToLine(IDataReader r, int startAtIndex)
        {
            var line = new T();
            for (var i = 0; i < _fieldInfos.Count; i++)
                ReaderToField(r, i + startAtIndex, _fieldInfos[i], line);
            if (IDField != null)
                ReaderToField(r, _fieldInfos.Count + startAtIndex, idFieldInfo, line);
            if (SmartUpdate && line is ReflectedTableLine rtl) {
                rtl.__smartUpdate_LastValues = new Dictionary<string, object>();
                for (int i=0;i<_fieldInfos.Count;i++)
                    rtl.__smartUpdate_LastValues.Add(_fieldInfos[i].Name, _fieldInfos[i].GetValue(line));
            }
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
                for (var i = 0; i < _fieldInfos.Count; i++)
                    if (_fieldInfos[i].FieldType == typeof(string)) {
                        likeField = _dbFieldNames[i];
                        break;
                    }

            return GetLikeStatement(likeField, indexValuePattern);
        }

        public int CountQuery(string criteria)
        {
            var useField = (IDField ?? IndexField) ?? _dbFieldNames[0];
            return MyDataSource.ExecuteScalarSql("select count(" + useField + ") from " + TableName + " where " +
                                                 criteria);
        }

        public int Sum(string field, string criteria = null)
        {
            string sql = "select sum(" + field + ") from " + TableName;
            if (!string.IsNullOrEmpty(criteria))
                sql += " where " + criteria;
            return MyDataSource.ExecuteScalarSql(sql);
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
        public IEnumerable<T> ByFieldEn(string fieldName, object fieldValue) =>
            Select(fieldName + "=" + DataSource.ValueToSql(fieldValue));

        public JoinSet<T> ByFieldSet(string fieldName, object fieldValue) =>
            new JoinSet<T>(fieldName, (int) fieldValue, this);

        public List<T> ByField(int topCount, string fieldName, object fieldValue) =>
            SelectList(topCount, fieldName + "=" + DataSource.ValueToSql(fieldValue));
        public IEnumerable<T> ByFieldEn(int topCount, string fieldName, object fieldValue) =>
            Select(topCount, fieldName + "=" + DataSource.ValueToSql(fieldValue));

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

        public void GetInstanceValues(T item, out FieldInfo[] fieldsInfo, out string[] fieldsNames, out object[] fieldsValues, bool includeID = false)
        {
            var fieldsCopy = new List<FieldInfo>(_fieldInfos);
            var fieldsNamesCopy = new List<string>(_dbFieldNames);

            bool hasId = IDField != null;
            if (hasId && includeID)
            {
                fieldsCopy.Add(idFieldInfo);
                fieldsNamesCopy.Add(IDField);
            }

            fieldsValues = new object[fieldsCopy.Count];
            for (var i = 0; i < fieldsCopy.Count; i++)
                fieldsValues[i] = fieldsCopy[i].GetValue(item);

            fieldsInfo = fieldsCopy.ToArray();
            fieldsNames = fieldsNamesCopy.ToArray();
        }

        public void GetFieldsArraysWithId(out FieldInfo[] fieldsInfo, out string[] fieldNames, out bool hasId)
        {
            var fieldsCopy = new List<FieldInfo>(_fieldInfos);
            var fieldsNamesCopy = new List<string>(_dbFieldNames);
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