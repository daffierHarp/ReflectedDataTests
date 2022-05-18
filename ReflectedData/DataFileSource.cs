
#region using

using System;
using System.Data.Common;
using System.Data.OleDb;
using System.IO;

#endregion

namespace ReflectedData
{
    public enum DataFileType
    {
        Access2007,
        AccessMdb,
        // ReSharper disable InconsistentNaming
        Excel2007_Xlsx,
        Excel2007_Xlsb,
        Excel2007_Xlsm,
        Excel,
        TextComaDelimited,
        // ReSharper restore InconsistentNaming

    }

    /// <summary>
    ///     Simplify access to Microsoft Access files - or Excel or ASCII data sources
    /// </summary>
    public class DataFileSource : DataSource
    {
        // TODO: create a pool of connections and remove Clone and ReuseConnection flags...
        readonly string _filePath;
        public bool ExcelDataAsString;
        public string FilePath => _filePath;

        /// <summary>
        ///     "HDR=Yes;" indicates that the first row contains column-names, not data. "HDR=No;" indicates the opposite.
        /// </summary>
        public bool ExcelHeaders = true;

        public DataFileType FileType = DataFileType.Access2007;

        /// <summary>
        ///     Using a file's extension, will deduct the right file type
        /// </summary>
        /// <param name="filePath"></param>
        public DataFileSource(string filePath)
        {
            this._filePath = filePath;
            switch (Path.GetExtension(filePath).ToLower()) {
                case ".txt":
                case ".asc":
                case ".csv":
                    FileType = DataFileType.TextComaDelimited;
                    break;
                case ".mdb":
                    FileType = DataFileType.AccessMdb;
                    break;
                // ReSharper disable StringLiteralTypo
                case ".accdb":
                    FileType = DataFileType.Access2007;
                    break;
                case ".xlsb":
                    FileType = DataFileType.Excel2007_Xlsb;
                    break;
                case ".xlsm":
                    FileType = DataFileType.Excel2007_Xlsm;
                    break;
                case ".xlsx":
                    FileType = DataFileType.Excel2007_Xlsx;
                    break;
                case ".xls":
                    FileType = DataFileType.Excel;
                    break;
                // ReSharper restore StringLiteralTypo

            }
        }

        /// <summary>
        ///     Open a new connection to the data file
        /// </summary>
        /// <returns></returns>
        public override DbConnection NewConnection()
        {
            // ReSharper disable StringLiteralTypo
            var connString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + _filePath;
            var hdr = ExcelHeaders ? "Yes" : "No";
            var imex = ExcelDataAsString ? "IMEX=1;ImportMixedTypes=Text;" : "";
            switch (FileType) {
                case DataFileType.AccessMdb:
                    connString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + _filePath +
                                 ";User Id=admin;Password=;";
                    break;
                case DataFileType.Excel2007_Xlsb:
                    connString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + _filePath +
                                 ";Extended Properties=\"Excel 12.0;HDR=" + hdr + ";" + imex + "\";";
                    break;
                case DataFileType.Excel2007_Xlsm:
                    connString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + _filePath +
                                 ";Extended Properties=\"Excel 12.0 Macro;HDR=" + hdr + ";" + imex + "\";";
                    break;
                case DataFileType.Excel2007_Xlsx:
                    connString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + _filePath +
                                 ";Extended Properties=\"Excel 12.0 Xml;HDR=" + hdr + ";" + imex + "\";";
                    break;
                case DataFileType.Excel:
                    connString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + _filePath +
                                 ";Extended Properties=\"Excel 8.0;HDR=" + hdr + ";" + imex + "\";";
                    break;
                case DataFileType.TextComaDelimited:
                    // connString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + _filePath +
                    connString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + _filePath +
                                  ";Extended Properties=\"text;HDR=Yes;FMT=Delimited\";";
                    break;
            }
            // ReSharper restore StringLiteralTypo

            var result = new OleDbConnection(connString);
            result.Open();
            return result;
        }

        /// <summary>
        ///     If you try to run several parallel queries on threads, and are reusing connection, use clones
        /// </summary>
        /// <returns></returns>
        public override DataSource Clone() =>
            new DataFileSource(_filePath) {
                ExcelDataAsString = ExcelDataAsString,
                ReuseConnection = ReuseConnection,
                ExcelHeaders = ExcelHeaders
            };


        protected override DbCommand createNewCommand(string sql, DbConnection cn) =>
            new OleDbCommand(sql, (OleDbConnection) cn);

        protected override DbCommand createNewCommand(string sql, DbConnection cn, Type[] parameters)
        {
            var result = new OleDbCommand(sql, (OleDbConnection) cn);
            for (var i = 0; i < parameters.Length; i++) {
                var dbt = typeToOledbType(parameters[i]);
                if (dbt == OleDbType.VarChar)
                    result.Parameters.Add("@F" + i, dbt, 250);
                else
                    result.Parameters.Add("@F" + i, dbt);
            }

            return result;
        }

        static OleDbType typeToOledbType(Type t)
        {
            if (t == typeof(int) || t == typeof(int?))
                return OleDbType.Integer; // "int";
            if (t == typeof(long) || t == typeof(long?))
                return OleDbType.BigInt; //64-bit int "bigint"
            if (t == typeof(DateTime) || t == typeof(DateTime?))
                return OleDbType.DBTimeStamp; // "datetime";

            if (t == typeof(bool) || t == typeof(bool?))
                return OleDbType.Boolean; //"bit";

            /*if (FileType != DataFileType.Access2007 && FileType != DataFileType.AccessMdb)
            {
                return "varchar(250)";
            }*/
            if (t == typeof(string))
                return OleDbType.VarChar; //"varchar(100)";
            if (t == typeof(float) || t == typeof(float?))
                return OleDbType.Single; //"float";
            if (t == typeof(decimal) || t == typeof(decimal?))
                return OleDbType.Currency; // "decimal";
            if (t == typeof(byte) || t == typeof(byte?))
                return OleDbType.TinyInt; //"tinyint";
            return OleDbType.BSTR; //"text";
        }

        protected override DbDataAdapter createNewDbDataAdapter(string sql, DbConnection c) =>
            new OleDbDataAdapter(sql, (OleDbConnection) c);

        protected override DbCommandBuilder createCommandBuilder(DbDataAdapter adapter) =>
            new OleDbCommandBuilder((OleDbDataAdapter) adapter);

        public override string DotNetType_to_dataType(Type t)
        {
            if (t == typeof(int))
                return "int";
            if (t == typeof(long))
                return "bigint";
            if (t == typeof(DateTime))
                return "datetime";

            if (t == typeof(bool))
                return "bit";

            /*if (FileType != DataFileType.Access2007 && FileType != DataFileType.AccessMdb)
            {
                return "varchar(250)";
            }*/
            if (t == typeof(string))
                return "varchar(100)";
            if (t == typeof(float))
                return "single";
            if (t == typeof(double))
                return "double";
            if (t == typeof(bool))
                return "smallint";
            if (t == typeof(decimal))
                return "decimal";
            if (t == typeof(byte))
                return "tinyint";
            return "text";
        }

        public override string GetConstraint_Identity()
        {
            switch (FileType) {
                case DataFileType.Access2007:
                    return " PRIMARY KEY AUTOINCREMENT";
                case DataFileType.AccessMdb:
                    return " IDENTITY PRIMARY KEY";
            }

            return "";
        }

        public override string GetConstraint_NotNull()
        {
            if (FileType != DataFileType.AccessMdb && FileType != DataFileType.Access2007)
                return "";
            return " NOT NULL";
        }
    }
}