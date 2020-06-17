
#region using

using System;
using System.Data.Common;
using System.Data.Odbc;
using System.Data.OleDb;
using System.Data.SqlClient;
// ReSharper disable UnusedMember.Global
// ReSharper disable MergeCastWithTypeCheck

#endregion

namespace ReflectedData
{
    /// <summary>
    ///     Provide a source for reflected tables, using an open connection
    /// </summary>
    /// <remarks>Supports OleDb, ODBC, Sql - other clients will require their own implementation</remarks>
    public class ConnectionSource : DataSource
    {
        public ConnectionSource(DbConnection cn)
        {
            ReuseConnection = true;
            recycledConnection = cn;
        }

        public override DataSource Clone() =>
            throw new NotImplementedException("Clone not allowed on ConnectionSource, reusing connection");

        public override void Dispose()
        {
            //base.Dispose();
        }

        protected override DbConnection getNewOrRecycledConnection() => recycledConnection;

        protected override void closeConnectionUnlessReuse(DbConnection cn)
        {
            //base.closeConnectionUnlessReuse(cn);
        }

        public override DbConnection NewConnection() => recycledConnection;

        protected override DbCommand createNewCommand(string sql, DbConnection cn)
        {
            if (cn is SqlConnection)
                return new SqlCommand(sql, (SqlConnection) cn);
            if (cn is OleDbConnection)
                return new OleDbCommand(sql, (OleDbConnection) cn);
            if (cn is OdbcConnection)
                return new OdbcCommand(sql, (OdbcConnection) cn);


            throw new NotImplementedException();
        }

        protected override DbCommand createNewCommand(string sql, DbConnection cn, Type[] parameters)
        {
            var result = createNewCommand(sql, cn);
            /*
            for (int i = 0; i < parameters.Length; i++)
            {
                SqlDbType dbt = typeToSqlDbType(parameters[i]);
                if (dbt == SqlDbType.VarChar)
                    result.Parameters.Add("@F" + i, dbt, 250);
                else
                    result.Parameters.Add("@F" + i, dbt);
            }*/
            throw new NotImplementedException();
        }

        protected override DbDataAdapter createNewDbDataAdapter(string sql, DbConnection cn)
        {
            if (cn is SqlConnection)
                return new SqlDataAdapter(sql, (SqlConnection) cn);
            if (cn is OleDbConnection)
                return new OleDbDataAdapter(sql, (OleDbConnection) cn);
            if (cn is OdbcConnection)
                return new OdbcDataAdapter(sql, (OdbcConnection) cn);
            throw new NotImplementedException();
        }

        protected override DbCommandBuilder createCommandBuilder(DbDataAdapter adapter)
        {
            if (adapter is SqlDataAdapter)
                return new SqlCommandBuilder((SqlDataAdapter) adapter);
            if (adapter is OleDbDataAdapter)
                return new OleDbCommandBuilder((OleDbDataAdapter) adapter);
            if (adapter is OdbcDataAdapter)
                return new OdbcCommandBuilder((OdbcDataAdapter) adapter);

            throw new NotImplementedException();
        }
    }
}