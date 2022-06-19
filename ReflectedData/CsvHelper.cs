using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ReflectedData
{
    public static class CsvHelper
    {
        
        public static string CsvReplaceCommaWith = "\u0375";//use greek lower numeral sign instead of comma, they look the same. Alternatively, consider URL encode replacement: "%2C";
        public static string CsvReplaceNewLineWith = "\u00B6"; // use Pilcrow sign for new line. Alternatively, consider &#xD;&#xA; - xml attribute style for equivalent values of the \r\n
        public static string CsvReplaceDblQuoteWith = "\u201F"; // use alternative quote sign
        /// <summary>
        /// utilize CsvReplaceCommaWith to replace commas and make string value of CSV valid. By default the static value is a greek-lower-numeral which looks just like comma
        /// </summary>
        public static string AsCsvField1(this string fieldValue) => fieldValue.Replace(",", CsvReplaceCommaWith).Replace("\r\n", CsvReplaceNewLineWith).Replace("\"", CsvReplaceDblQuoteWith);
        /// <summary>
        /// surrounds string with double-quotes, replace internal quotes with double-double quotes
        /// </summary><remarks>quote replacement string cannot be parsed in place, cannot just split on comma for each line, must call ParseCsv to read all lines and values</remarks>
        public static string AsCsvField2(this string fieldValue) => $"\"{fieldValue.Replace("\"","\"\"")}\"";
        /// <summary>
        /// parse field value of CSV that is encoded AsCsvField1
        /// </summary>
        public static string FromCsvField1(this string encodedCsvFieldValue) => encodedCsvFieldValue.Replace(CsvReplaceCommaWith, ",").Replace(CsvReplaceNewLineWith, "\r\n").Replace(CsvReplaceDblQuoteWith, "\"");

        public static IEnumerable<string[]> ParseCsv(StreamReader r)
        {
            int atLine = 0;
            while (!r.EndOfStream) {
                var l = r.ReadLine();
                if (l==null)
                    yield break;
                var vals = l.Split(',');
                var valsResult = new List<string>();
                for (var vi = 0; vi < vals.Length; vi++) {
                    var v = vals[vi];
                    if (!v.StartsWith("\"")) {
                        valsResult.Add(v.FromCsvField1());
                        continue;
                    }

                    var sb = new StringBuilder();
                    bool inStr = true;
                    while (true) {
                        for (int ci = 1; ci < v.Length; ci++) {
                            var c = v[ci];
                            if (c == '\"') {
                                if (!inStr)
                                    sb.Append('\"');
                                inStr = !inStr;
                                continue;
                            }

                            if (inStr) {
                                sb.Append(c);
                                continue;
                            }

                            if (char.IsWhiteSpace(c)) continue;
                            throw new Exception($"Unexpected character {c} out of string at line {atLine}");
                        }

                        if (!inStr) break; // ended with a dbl-quote
                        vi++;
                        if (vi != vals.Length)
                            sb.Append(",");
                        else {
                            vi = 0;
                            l = r.ReadLine();
                            if (l == null) throw new Exception("Invalid CSV encoding at line " + atLine);

                            sb.Append("\r\n");
                            atLine++;
                            vals = l.Split(',');
                        }

                        v = vals[vi];
                    }
                    valsResult.Add(sb.ToString());
                }

                yield return valsResult.ToArray();
                atLine++;
            }
        }
    }
}
