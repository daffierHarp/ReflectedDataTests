#region using

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;
// ReSharper disable UnusedMemberInSuper.Global
// ReSharper disable InconsistentNaming
// ReSharper disable UnusedMember.Global

#endregion

namespace ReflectedData
{
    // TODO: allow timer based auto-disconnecting source
    public abstract class DataSource : IDisposable
    {
        public static bool SmartUpdates = true;
        TablesCache _tablesCache;

        readonly Dictionary<IDataReader, List<IDisposable>> _readerItems =
            new Dictionary<IDataReader, List<IDisposable>>();

        protected DbConnection recycledConnection;

        /// <summary>
        ///     Improve data access speed and reduce hard-drive access by setting this to true. Through a single
        ///     reused connection, you cannot run parallel queries. To resolve this, create a clone at
        ///     perpetrate threads. This will insure usage of separate connections.
        /// </summary>
        public bool ReuseConnection = true;

        public TablesCache Tables
        {
            get
            {
                lock (this) {
                    if (_tablesCache == null)
                        _tablesCache = new TablesCache(this);
                }

                return _tablesCache;
            }
        }

        #region IDisposable Members

        public virtual void Dispose()
        {
            FlushReaders();
            recycledConnection?.Close();
            recycledConnection = null;
        }

        #endregion

        public abstract DbConnection NewConnection();
        public abstract DataSource Clone();
        protected abstract DbCommandBuilder createCommandBuilder(DbDataAdapter adapter);

        public void CloseReusedConnection()
        {
            lock (this) {
                if (recycledConnection == null)
                    return;
                recycledConnection.Close();
                recycledConnection = null;
            }
        }

        public void Open()
        {
            getNewOrRecycledConnection();
        }
        protected virtual DbConnection getNewOrRecycledConnection()
        {
            lock (this) {
                if (!ReuseConnection)
                    return NewConnection();
                if (recycledConnection != null) {
                    if (recycledConnection.State != ConnectionState.Open)
                        recycledConnection = NewConnection();
                    return recycledConnection;
                }

                return recycledConnection = NewConnection();
            }
        }

        protected virtual void closeConnectionUnlessReuse(DbConnection cn)
        {
            if (ReuseConnection)
                return;
            cn.Close();
        }

        protected abstract DbDataAdapter createNewDbDataAdapter(string sql, DbConnection c);

        /// <summary>
        ///     DataSets are friendly to .net control's binding features and for grids. Most of the rest of this
        ///     assembly provides usage of reflection to deduct data side schema.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public DataSet GetDataSet(string sql)
        {
            var c = getNewOrRecycledConnection();
            var adapter = createNewDbDataAdapter(sql, c);
            var result = new DataSet();
            adapter.Fill(result);
            adapter.Dispose();
            closeConnectionUnlessReuse(c);
            return result;
        }

        /// <summary>
        ///     return an enumerable over the rows of the first table returned in the data set for an sql statement.
        ///     This is useful for linq queries without reflection involved.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public DataSetWrapper GetDataSetWrapper(string sql) => new DataSetWrapper(GetDataSet(sql));

        public DataSet GetDataSetWithCommands(string singleTableSql, out DbDataAdapter adapter)
        {
            var c = getNewOrRecycledConnection();
            adapter = createNewDbDataAdapter(singleTableSql, c);
            var cmdBuilder = createCommandBuilder(adapter);
            var dataSet = new DataSet();
            adapter.Fill(dataSet);
            cmdBuilder.GetUpdateCommand();
            cmdBuilder.GetDeleteCommand();
            cmdBuilder.GetInsertCommand();
            closeConnectionUnlessReuse(c);
            return dataSet;
        }

        protected abstract DbCommand createNewCommand(string sql, DbConnection cn);
        protected abstract DbCommand createNewCommand(string sql, DbConnection cn, Type[] parameters);

        /// <summary>
        ///     execute non query, return number of rows affected
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public int ExecuteSql(string sql)
        {
            var cn = getNewOrRecycledConnection();
            var cmd = createNewCommand(sql, cn);
            cmd.Prepare();
            var result = cmd.ExecuteNonQuery();
            closeConnectionUnlessReuse(cn);
            return result;
        }

        public int ExecuteScalarSql(string sql)
        {
            var cn = getNewOrRecycledConnection();
            var cmd = createNewCommand(sql, cn);
            cmd.Prepare();
            var result = cmd.ExecuteScalar();
            if (result == null || result is DBNull) return 0;
            var intResult = Convert.ToInt32(result);
            closeConnectionUnlessReuse(cn);
            return intResult;
        }

        public object ExecuteScalarSqlToObject(string sql)
        {
            var cn = getNewOrRecycledConnection();
            var cmd = createNewCommand(sql, cn);
            cmd.Prepare();
            var result = cmd.ExecuteScalar();
            closeConnectionUnlessReuse(cn);
            return result;
        }

        /// <summary>
        ///     execute an sql, expecting 1 line returned, and 1 field, which is string
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public string ExecuteScalarSqlAsString(string sql)
        {
            var cn = getNewOrRecycledConnection();
            var cmd = createNewCommand(sql, cn);
            cmd.Prepare();
            var result = Convert.ToString(cmd.ExecuteScalar());
            closeConnectionUnlessReuse(cn);
            return result;
        }

        /// <summary>
        ///     readers returned from ExecuteReader should be disposed through DisposeReader
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public IDataReader ExecuteReader(string sql)
        {
            lock (this) {
                var cn = getNewOrRecycledConnection();
                var cmd = createNewCommand(sql, cn);
                IDataReader result = cmd.ExecuteReader();
                var listToDispose = new List<IDisposable> {cmd};
                if (!ReuseConnection)
                    listToDispose.Add(cn);
                _readerItems.Add(result, listToDispose);
                return result;
            }
        }

        /// <summary>
        ///     clear all instances of objects created by ExecuteReader
        /// </summary>
        /// <param name="r"></param>
        public void DisposeReader(IDataReader r)
        {
            lock (this) {
                r.Dispose();
                foreach (var disp in _readerItems[r])
                    disp.Dispose();
                _readerItems.Remove(r);
            }
        }

        /// <summary>
        ///     When several readers are executed in parallel (not reusing connection), they can all
        ///     be disposed together here
        /// </summary>
        public void FlushReaders()
        {
            lock (this) {
                foreach (var r in _readerItems.Keys) {
                    r.Dispose();
                    foreach (var disp in _readerItems[r])
                        disp.Dispose();
                }

                _readerItems.Clear();
            }
        }

        /// <summary>
        ///     Create a string for usage in an SQL query from value at v
        /// </summary>
        /// <param name="v"></param>
        /// <returns></returns>
        public static string ValueToSql(object v)
        {
            if (v == null) return "Null";
            if (v is DateTime dateTime)
                return "'" + dateTime.ToString("yyyy-MM-dd HH:mm:ss") + "'";
            if (v is string s)
                return "'" + s.Replace("'", "''") + "'";
            if (v is bool b) {
                if (b)
                    return "1";
                return "0";
            }

            return v.ToString();
        }

        /// <summary>
        ///     construct and execute an "update" sql query
        /// </summary>
        /// <param name="table"></param>
        /// <param name="idField"></param>
        /// <param name="rowId"></param>
        /// <param name="fields"></param>
        /// <param name="values"></param>
        public void Update(string table, string idField, object rowId, string[] fields, object[] values)
        {
            var sqlBuilder = new StringBuilder();
            sqlBuilder.Append("UPDATE " + table + " SET ");
            for (var i = 0; i < fields.Length; i++) {
                sqlBuilder.Append(fields[i] + " = ");
                if (values[i] == null) values[i] = "";
                sqlBuilder.Append(ValueToSql(values[i]));
                if (i < fields.Length - 1)
                    sqlBuilder.Append(", ");
            }

            sqlBuilder.Append(" WHERE " + idField + "=" + ValueToSql(rowId) + ";");
            ExecuteSql(sqlBuilder.ToString());
        }

        /// <summary>
        ///     construct and execute an "insert" sql query
        /// </summary>
        /// <param name="table"></param>
        /// <param name="fields"></param>
        /// <param name="values"></param>
        /// <param name="getIdentity"></param>
        /// <returns>if getIdentity is true, returns the auto-number identity value</returns>
        /// <remarks>This function is virtual, to allow the ID be returned in one call for non access files</remarks>
        public virtual int Insert(string table, string[] fields, object[] values, bool getIdentity)
        {
            var sql = GetInsertSql(table, fields, values);
            var cn = getNewOrRecycledConnection();
            var cmd = createNewCommand(sql, cn);
            cmd.Prepare();
            cmd.ExecuteNonQuery();
            if (!getIdentity) {
                cmd.Dispose();
                closeConnectionUnlessReuse(cn);
                return -1;
            }

            var cmd2 = createNewCommand("SELECT @@IDENTITY;", cn);
            var result = Convert.ToInt32(cmd2.ExecuteScalar());
            cmd.Dispose();
            cmd2.Dispose();
            closeConnectionUnlessReuse(cn);
            return result;
        }

        // TODO: implement this better for sql server
        public virtual void InsertBulk(string table, string[] fields, Type[] fieldTypes, object[,] values)
        {
            var sql = GetInsertSqlWithParams(table, fields);
            var cn = getNewOrRecycledConnection();
            var cmd = createNewCommand(sql, cn, fieldTypes);
            cmd.Prepare();
            for (var i = 0; i < values.GetLength(0); i++) {
                for (var f = 0; f < fields.Length; f++) cmd.Parameters["@F" + f].Value = values[i, f];
                cmd.ExecuteNonQuery();
            }

            cmd.Dispose();
            closeConnectionUnlessReuse(cn);
        }

        public static string GetInsertSql(string table, string[] fields, object[] values)
        {
            var sqlBuilder = new StringBuilder();
            sqlBuilder.Append("INSERT INTO " + table + " ( ");
            sqlBuilder.Append(string.Join(",", fields.ToArray()));
            sqlBuilder.Append(" ) VALUES (");
            for (var i = 0; i < values.Length; i++) {
                sqlBuilder.Append(ValueToSql(values[i]));
                if (i < fields.Length - 1)
                    sqlBuilder.Append(", ");
            }

            sqlBuilder.Append(");");
            var sql = sqlBuilder.ToString();
            return sql;
        }

        /// <summary>
        ///     creates an insert sql for all fields, each field is named F + index, like @F0, @F1 etc
        /// </summary>
        /// <param name="table"></param>
        /// <param name="fields"></param>
        /// <returns></returns>
        public static string GetInsertSqlWithParams(string table, string[] fields)
        {
            var sqlBuilder = new StringBuilder();
            sqlBuilder.Append("INSERT INTO " + table + " ( ");
            sqlBuilder.Append(string.Join(",", fields.ToArray()));
            sqlBuilder.Append(" ) VALUES (");
            for (var i = 0; i < fields.Length; i++) {
                sqlBuilder.Append("@F" + i);
                if (i < fields.Length - 1)
                    sqlBuilder.Append(", ");
            }

            sqlBuilder.Append(");");
            var sql = sqlBuilder.ToString();
            return sql;
        }

        /*
        public void InsertWithParams(string table, string[] fields, object[,] values, bool getIdentity)
        {
            string sql = GetInsertSqlWithParams(table, fields);
            DbConnection cn = getNewOrRecycledConnection();
            DbCommand cmd = createNewCommand(sql, cn);
            OleDbCommand oleCmd = cmd as OleDbCommand;
            new OleDbParameter("@F0", 
            //cmd.Parameters.Add(
            cmd.Prepare();
            cmd.ExecuteNonQuery();
            if (!getIdentity)
            {
                cmd.Dispose();
                closeConnectionUnlessReuse(cn);
                return -1;
            }
            DbCommand cmd2 = createNewCommand("SELECT @@IDENTITY;", cn);
            int result = Convert.ToInt32(cmd2.ExecuteScalar());
            cmd.Dispose();
            cmd2.Dispose();
            closeConnectionUnlessReuse(cn);
            return result;
        }*/
        /// <summary>
        ///     construct and execute a "delete" sql query
        /// </summary>
        /// <param name="table"></param>
        /// <param name="idField"></param>
        /// <param name="deleteValue"></param>
        public bool Delete(string table, string idField, object deleteValue) =>
            ExecuteSql("DELETE FROM " + table + " WHERE " + idField + "=" + ValueToSql(deleteValue) + ";") > 0;

        public bool Delete(string table, string whereSection) =>
            ExecuteSql("DELETE FROM " + table + " WHERE " + whereSection + ";") > 0;

        /// <summary>
        ///     Utilize reflection, pick fields, and build a query with the "from" and "where" section
        ///     provided in the parameters.
        ///     Use this function to extract data relevant to joined parent/child and other
        ///     complex relationships between tables.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fromSection"></param>
        /// <param name="whereSection"></param>
        /// <returns></returns>
        public List<T> SelectList<T>(string fromSection, string whereSection) where T : class, new()
        {
            getTypeRelationFields<T>(out var fields, out var fieldNames);
            var buildWhere = whereSection + "" == "" ? "" : " WHERE " + whereSection;
            var r = ExecuteReader(
                "SELECT " + string.Join(",", fieldNames.ToArray()) +
                " FROM " + fromSection +
                buildWhere + ";");
            var result = ReaderToList<T>(fields, r);
            DisposeReader(r);
            return result;
        }

        public IEnumerable<T> Select<T>(string fromSection, string whereSection) where T : class, new()
        {
            getTypeRelationFields<T>(out var fields, out var fieldNames);
            var buildWhere = whereSection + "" == "" ? "" : " WHERE " + whereSection;
            var sql = "SELECT " + string.Join(",", fieldNames.ToArray()) +
                      " FROM " + fromSection +
                      buildWhere + ";";
            return new QueryNonTableEnumerable<T>(this, fields, sql);
        }

        public QueryNonTableSet<T> SelectSet<T>(string fromSection, string whereSection) where T : class, new() =>
            new QueryNonTableSet<T>(this, true, fromSection, whereSection, null);

        public QueryNonTableSet<T> SelectUnattributedSet<T>(string fromSection, string whereSection)
            where T : class, new() => new QueryNonTableSet<T>(this, false, fromSection, whereSection, null);

        /// <summary>
        ///     Use reflection of type T to deduct the "select" section of an sql query, but does
        ///     not expect any attributes. All public fields of class are expected to be fields of
        ///     query
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fromSection"></param>
        /// <param name="whereSection"></param>
        /// <returns></returns>
        public List<T> SelectUnattributedList<T>(string fromSection, string whereSection) where T : class, new()
        {
            getTypeRelationFieldsNoAttributes<T>(out var fields, out var fieldNames);
            var buildWhere = whereSection + "" == "" ? "" : " WHERE " + whereSection;
            var r = ExecuteReader(
                "SELECT " + string.Join(",", fieldNames.ToArray()) +
                " FROM " + fromSection +
                buildWhere + ";");
            var result = ReaderToList<T>(fields, r);
            DisposeReader(r);
            return result;
        }

        public IEnumerable<T> SelectUnattributed<T>(string fromSection, string whereSection) where T : class, new()
        {
            getTypeRelationFieldsNoAttributes<T>(out var fields, out var fieldNames);
            var buildWhere = whereSection + "" == "" ? "" : " WHERE " + whereSection;
            var sql = "SELECT " + string.Join(",", fieldNames.ToArray()) +
                      " FROM " + fromSection +
                      buildWhere + ";";
            return new QueryNonTableEnumerable<T>(this, fields, sql);
        }

        internal static void getTypeRelationFields<T>(out List<FieldInfo> fields, out List<string> fieldNames)
            where T : class, new()
        {
            fields = new List<FieldInfo>();
            fieldNames = new List<string>();
            var ttype = typeof(T);
            var classAttributes = ttype.GetCustomAttributes(typeof(DataRecordAttribute), true);
            var allFields = false;
            if (classAttributes.Length > 0) {
                var dataRecordAttr = (DataRecordAttribute) classAttributes[0];
                allFields = dataRecordAttr.AllFields;
            }

            foreach (var finfo in ttype.GetFields(BindingFlags.Public | BindingFlags.Instance |
                                                  BindingFlags.FlattenHierarchy)) {
                var attributes = finfo.GetCustomAttributes(typeof(DataFieldAttribute), true);
                var fieldName = finfo.Name;
                if (attributes.Length > 0) {
                    var dataFieldAttr = (DataFieldAttribute) attributes[0];
                    if (dataFieldAttr.Ignore)
                        continue;
                    if (dataFieldAttr.Rename != null)
                        fieldName = ((DataFieldAttribute) attributes[0]).Rename;
                    if (dataFieldAttr.IsID) { }

                    if (dataFieldAttr.IsIndex) { }
                } else if (!allFields) {
                    continue;
                }

                fields.Add(finfo);
                fieldNames.Add(fieldName);
            }
        }

        internal static void getTypeRelationFieldsNoAttributes<T>(out List<FieldInfo> fields,
            out List<string> fieldNames) where T : class, new()
        {
            fields = new List<FieldInfo>();
            fieldNames = new List<string>();
            var ttype = typeof(T);
            foreach (var finfo in ttype.GetFields(BindingFlags.Public | BindingFlags.Instance |
                                                  BindingFlags.FlattenHierarchy)) {
                var fieldName = finfo.Name;
                fields.Add(finfo);
                fieldNames.Add(fieldName);
            }
        }

        /// <summary>
        ///     run any complex query and map results into a list of lines of type T.
        ///     The mapping is order based, not name based
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <returns></returns>
        public List<T> SelectList<T>(string sql) where T : class, new()
        {
            getTypeRelationFields<T>(out var fields, out _);
            var r = ExecuteReader(sql);
            var result = ReaderToList<T>(fields, r);
            DisposeReader(r);
            return result;
        }

        public IEnumerable<T> Select<T>(string sql) where T : class, new()
        {
            getTypeRelationFields<T>(out var fields, out _);
            return new QueryNonTableEnumerable<T>(this, fields, sql);
        }

        public List<T> SelectUnattributedList<T>(string sql) where T : class, new()
        {
            getTypeRelationFieldsNoAttributes<T>(out var fields, out _);
            var r = ExecuteReader(sql);
            var result = ReaderToList<T>(fields, r);
            DisposeReader(r);
            return result;
        }

        public IEnumerable<T> SelectUnattributed<T>(string sql) where T : class, new()
        {
            getTypeRelationFieldsNoAttributes<T>(out var fields, out _);
            return new QueryNonTableEnumerable<T>(this, fields, sql);
        }

        /// <summary>
        ///     Match the ordered field info in the fields list to the column numbers in a line in reader.
        ///     fields should be from the class of type T.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fields"></param>
        /// <param name="r"></param>
        /// <returns></returns>
        public static List<T> ReaderToList<T>(List<FieldInfo> fields, IDataReader r) where T : class, new()
        {
            var result = new List<T>();
            while (r.Read()) {
                var line = readerToLine<T>(fields, r);
                result.Add(line);
            }

            return result;
        }

        internal static T readerToLine<T>(List<FieldInfo> fields, IDataReader r) where T : class, new()
        {
            var line = new T();
            for (var i = 0; i < fields.Count; i++) {
                var columnValue = ReflectedTable<T>.ReadColumn(r, i, fields[i].FieldType);
                if (columnValue == null && !ReflectedTable<T>.IsNullableType(fields[i].FieldType))
                    continue;
                fields[i].SetValue(line, columnValue);
            }

            return line;
        }

        public static string GetLineTableName(Type lineType)
        {
            var result = lineType.Name + "s";
            try {
                var pinfo = lineType.GetProperty("TableName", BindingFlags.Static | BindingFlags.Public);
                if (pinfo != null)
                    result = (string) pinfo.GetValue(null, null);
            } catch { }

            return result;
        }

        public List<T> TableToList<T>(string tableName) where T : class, new() => SelectList<T>(tableName, null);

        public ReflectedTable<T> Table<T>() where T : class, new() => Tables.Get<T>();

        public JoinedRecordSet<T1, T2> Join<T1, T2>(string onT2Field)
            where T1 : class, new()
            where T2 : class, new()
        {
            return new JoinedRecordSet<T1, T2>(this, new[] {onT2Field}, null, null);
        }

        public JoinedRecordSet<T1, JoinedRecord<T2, T3>> Join<T1, T2, T3>(string onT2Field, string onT3Field)
            where T1 : class, new()
            where T2 : class, new()
            where T3 : class, new()
        {
            return new JoinedRecordSet<T1, JoinedRecord<T2, T3>>(this, new[] {onT2Field, onT3Field}, null, null);
        }

        public JoinedRecordSet<T1, JoinedRecord<T2, JoinedRecord<T3, T4>>> Join<T1, T2, T3, T4>(string onT2Field,
            string onT3Field, string onT4Field)
            where T1 : class, new()
            where T2 : class, new()
            where T3 : class, new()
            where T4 : class, new()
        {
            return new JoinedRecordSet<T1, JoinedRecord<T2, JoinedRecord<T3, T4>>>(this,
                new[] {onT2Field, onT3Field, onT4Field}, null, null);
        }

        public virtual string DotNetType_to_dataType(Type t)
        {
            if (t == typeof(string))
                return "nvarchar(100)";
            if (t == typeof(int))
                return "int";
            if (t == typeof(long))
                return "bigint";
            if (t == typeof(float))
                return "float";
            if (t == typeof(bool))
                return "bit";
            if (t == typeof(DateTime))
                return "datetime";
            if (t == typeof(decimal))
                return "decimal";
            if (t == typeof(byte))
                return "tinyint";
            // ReSharper disable once StringLiteralTypo
            return "ntext";
        }

        public virtual string GetConstraint_Identity() => " PRIMARY KEY IDENTITY";

        public virtual string GetConstraint_NotNull() => " NOT NULL";

        public string[] GetDataTableNames()
        {
            var cn = getNewOrRecycledConnection();
            var tables = cn.GetSchema("Tables");
            /*foreach(System.Data.DataRow r in tables.Rows)
                foreach (System.Data.DataColumn c in tables.Columns)
                {
                    System.Diagnostics.Debug.WriteLine( c.ColumnName +"=" + r[c]);
                }*/
            var result = new List<string>();
            foreach (DataRow r in tables.Rows)
                if (r["TABLE_TYPE"] + "" == "TABLE" || r["TABLE_TYPE"] + "" == "BASE TABLE")
                    result.Add(r["TABLE_NAME"] + "");
            tables.Dispose();
            closeConnectionUnlessReuse(cn);
            return result.ToArray();
        }

        public class DataSetWrapper : IDisposable, IEnumerable<DataRow>
        {
            readonly List<DataRow> l;

            public DataSetWrapper(DataSet d)
            {
                DataSet = d;
                l = GetRowsList(0);
            }

            public int Columns => DataSet.Tables[0].Columns.Count;
            public int Count => l.Count;
            public DataSet DataSet { get; }

            #region IDisposable Members

            public void Dispose()
            {
                DataSet.Dispose();
                l.Clear();
            }

            #endregion

            #region IEnumerable<DataRow> Members

            public IEnumerator<DataRow> GetEnumerator() => l.GetEnumerator();

            #endregion

            #region IEnumerable Members

            IEnumerator IEnumerable.GetEnumerator() => l.GetEnumerator();

            #endregion

            public string ColumnName(int index) => DataSet.Tables[0].Columns[index].ColumnName;

            public List<DataRow> GetRowsList(int tableIndex) =>
                new List<DataRow>(DataSet.Tables[tableIndex].Rows.Cast<DataRow>());
        }

        public class TablesCache
        {
            readonly DataSource src;

            readonly Dictionary<string, object> tables =
                new Dictionary<string, object>(StringComparer.InvariantCultureIgnoreCase);

            internal TablesCache(DataSource src)
            {
                this.src = src;
            }

            public ReflectedTable<T> Get<T>() where T : class, new()
            {
                lock (this) {
                    var table = GetLineTableName(typeof(T));
                    if (tables.ContainsKey(table))
                        return (ReflectedTable<T>) tables[table];
                    var result = new ReflectedTable<T>(src);
                    tables.Add(table, result);
                    return result;
                }
            }

            public object Get(Type lineType)
            {
                lock (this) {
                    var table = GetLineTableName(lineType);
                    if (tables.ContainsKey(table))
                        return tables[table];
                    var rtable = typeof(ReflectedTable<>);
                    var tableOfLineType = rtable.MakeGenericType(lineType);
                    var result = Activator.CreateInstance(tableOfLineType, src);
                    tables.Add(table, result);
                    return result;
                }
            }
        }
    }
}