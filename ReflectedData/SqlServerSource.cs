
#region using

using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
// ReSharper disable UnusedMember.Global

#endregion

namespace ReflectedData
{
    /// <summary>
    ///     Sql Server Reflect-able Source
    /// </summary>
    public class SqlServerSource : DataSource
    {
        readonly string _passwd;
        public SqlConnectionStringBuilder UseConnectionStringBuilder;

        public SqlServerSource(string serverAddress, string database, string userName, string passwd)
        {
            ServerAddress = serverAddress;
            UserName = userName;
            this._passwd = passwd;
            Database = database;
        }

        /// <summary>
        ///     instantiate a helper for a connection to local host's sql-express service
        /// </summary>
        /// <param name="database"></param>
        public SqlServerSource(string database)
        {
            Database = database;
            // ReSharper disable once StringLiteralTypo
            ServerAddress = ".\\SQLEXPRESS";
        }

        public SqlServerSource(SqlConnectionStringBuilder connStrBuilder)
        {
            UseConnectionStringBuilder = connStrBuilder;
        }

        public string Database { get; set; }

        public string ServerAddress { get; }

        public string UserName { get; }

        public override DataSource Clone()
        {
            var result = new SqlServerSource(ServerAddress, Database, UserName, _passwd) {
                UseConnectionStringBuilder = UseConnectionStringBuilder
            };
            return result;
        }

        public override DbConnection NewConnection()
        {
            var cnStrBuilder = UseConnectionStringBuilder ?? new SqlConnectionStringBuilder("Data Source=.\\SQLEXPRESS;Integrated Security=True;Pooling=False");
            if (ServerAddress != null)
                cnStrBuilder.DataSource = ServerAddress;
            if (Database != null)
                cnStrBuilder.InitialCatalog = Database;

            if (UserName != null) {
                cnStrBuilder.UserID = UserName;
                cnStrBuilder.Password = _passwd;
            }

            var cn = new SqlConnection(cnStrBuilder.ConnectionString);
            cn.Open();
            return cn;
        }

        protected override DbCommand createNewCommand(string sql, DbConnection cn) =>
            new SqlCommand(sql, (SqlConnection) cn);

        protected override DbCommand createNewCommand(string sql, DbConnection cn, Type[] parameters)
        {
            var result = new SqlCommand(sql, (SqlConnection) cn);
            for (var i = 0; i < parameters.Length; i++) {
                var dbt = typeToSqlDbType(parameters[i]);
                if (dbt == SqlDbType.VarChar)
                    result.Parameters.Add("@F" + i, dbt, 250);
                else
                    result.Parameters.Add("@F" + i, dbt);
            }

            return result;
        }

        static SqlDbType typeToSqlDbType(Type t)
        {
            if (t == typeof(int) || t == typeof(int?))
                return SqlDbType.Int; // "int";
            if (t == typeof(DateTime) || t == typeof(DateTime?))
                return SqlDbType.DateTime; // "datetime";

            if (t == typeof(bool) || t == typeof(bool?))
                return SqlDbType.Bit; //"bit";
            if (t == typeof(string))
                return SqlDbType.VarChar; //"varchar(100)";
            if (t == typeof(float) || t == typeof(float?))
                return SqlDbType.Float; //"float";
            if (t == typeof(decimal) || t == typeof(decimal?))
                return SqlDbType.Decimal; // "decimal";
            if (t == typeof(byte) || t == typeof(byte?))
                return SqlDbType.TinyInt; //"tinyint";
            return SqlDbType.Text; //"text";
        }

        protected override DbDataAdapter createNewDbDataAdapter(string sql, DbConnection c) =>
            new SqlDataAdapter(sql, (SqlConnection) c);

        public override int Insert(string table, string[] fields, object[] values, bool getIdentity)
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

            sqlBuilder.Append(")");
            var cn = getNewOrRecycledConnection();
            if (!getIdentity) {
                sqlBuilder.Append(";");
                var cmd = createNewCommand(sqlBuilder.ToString(), cn);
                cmd.Prepare();
                cmd.ExecuteNonQuery();
                cmd.Dispose();
                closeConnectionUnlessReuse(cn);
                return -1;
            }

            sqlBuilder.Append(" SELECT @@IDENTITY;");
            var cmd2 = createNewCommand(sqlBuilder.ToString(), cn);
            var result = Convert.ToInt32(cmd2.ExecuteScalar());
            cmd2.Dispose();
            closeConnectionUnlessReuse(cn);
            return result;
        }

        protected override DbCommandBuilder createCommandBuilder(DbDataAdapter adapter) =>
            new SqlCommandBuilder((SqlDataAdapter) adapter);
    }
}