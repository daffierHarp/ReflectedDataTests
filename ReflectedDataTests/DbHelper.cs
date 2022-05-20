using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Helpers
{
    public enum DbFileType
    {
        Access2007,
        AccessMdb,
        // ReSharper disable InconsistentNaming
        Excel2007_Xlsx,
        Excel2007_Xlsb,
        Excel2007_Xlsm,
        Excel,
        TextComaDelimited,
        Unknown,
        // ReSharper restore InconsistentNaming

    }
    /// <summary>
    /// Encapsulate often used techniques for data but without a whole set of library like ReflectedData
    /// </summary>
    public static class DbHelper
    {
        public static DbFileType GetDbFileType(string filePath)
        {
            switch (Path.GetExtension(filePath).ToLower()) {
                case ".txt":
                case ".asc":
                case ".csv":
                    return DbFileType.TextComaDelimited;
                case ".mdb":
                    return DbFileType.AccessMdb;
                // ReSharper disable StringLiteralTypo
                case ".accdb":
                    return DbFileType.Access2007;
                case ".xlsb":
                    return DbFileType.Excel2007_Xlsb;
                case ".xlsm":
                    return DbFileType.Excel2007_Xlsm;
                case ".xlsx":
                    return DbFileType.Excel2007_Xlsx;
                case ".xls":
                    return DbFileType.Excel;
                // ReSharper restore StringLiteralTypo
            }
            return DbFileType.Unknown;
        }
        public static string GetDataFileConnectionString(string filePath, bool excelHeaders = true, bool excelDataAsString = false)
        {
            // ReSharper disable StringLiteralTypo
            var hdr = excelHeaders ? "Yes" : "No";
            var imex = excelDataAsString ? "IMEX=1;ImportMixedTypes=Text;" : "";
            switch (GetDbFileType(filePath)) {
                case DbFileType.Access2007: return "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + filePath;;
                case DbFileType.AccessMdb:
                    return "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + filePath +
                                 ";User Id=admin;Password=;";
                case DbFileType.Excel2007_Xlsb:
                    return "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + filePath +
                                 ";Extended Properties=\"Excel 12.0;HDR=" + hdr + ";" + imex + "\";";
                case DbFileType.Excel2007_Xlsm:
                    return "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + filePath +
                                 ";Extended Properties=\"Excel 12.0 Macro;HDR=" + hdr + ";" + imex + "\";";
                case DbFileType.Excel2007_Xlsx:
                    return "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + filePath +
                                 ";Extended Properties=\"Excel 12.0 Xml;HDR=" + hdr + ";" + imex + "\";";
                case DbFileType.Excel:
                    return "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + filePath +
                                 ";Extended Properties=\"Excel 8.0;HDR=" + hdr + ";" + imex + "\";";
                case DbFileType.TextComaDelimited:
                    // connString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + _filePath +
                    return "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + filePath +
                                  ";Extended Properties=\"text;HDR=Yes;FMT=Delimited\";";
            }
            // ReSharper restore StringLiteralTypo
            throw new Exception("Invalid file type");
        }
        public static OleDbConnection OpenDbFile(string filePath, bool excelHeaders = true, bool excelDataAsString = false)
        {
            var result = new OleDbConnection(GetDataFileConnectionString(filePath, excelHeaders, excelDataAsString));
            result.Open();
            return result;
        }

        public static DataSet GetDataSet(this OleDbConnection cn, string sql)
        {
            var adapter = new OleDbDataAdapter(sql, cn);
            var result = new DataSet();
            adapter.Fill(result);
            adapter.Dispose();
            return result;
        }
        public static IDataReader Query(this OleDbConnection cn, string sql)
        {
            var cmd = new OleDbCommand(sql, cn);
            return cmd.ExecuteReader();
        }
        public static int Exec(this OleDbConnection cn, string sql)
        {
            var cmd = new OleDbCommand(sql, cn);
            return cmd.ExecuteNonQuery();
        }
        public static string[] GetTableNames(this OleDbConnection cn)
        {
            var tables = cn.GetSchema("Tables");
            var result = new List<string>();
            foreach (DataRow r in tables.Rows)
                if (r["TABLE_TYPE"] + "" == "TABLE" || r["TABLE_TYPE"] + "" == "BASE TABLE")
                    result.Add(r["TABLE_NAME"] + "");
            tables.Dispose();
            return result.ToArray();
        }
    }
}
