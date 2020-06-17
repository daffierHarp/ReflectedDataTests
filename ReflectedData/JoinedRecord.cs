
#region using

using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Text;
// ReSharper disable InconsistentNaming
// ReSharper disable StaticMemberInGenericType
// ReSharper disable NotAccessedField.Local
// ReSharper disable UnusedMember.Global
// ReSharper disable UnusedMember.Local

#endregion

namespace ReflectedData
{
    /// <summary>
    ///     A result record of a join query
    /// </summary>
    public class JoinedRecord<TL, TR>
        where TL : class, new()
        where TR : class, new()
    {
        static FieldInfo[] lFieldsInfo, rFieldsInfo;
        static string[] lFieldsNames, rFieldsNames;
        static bool lHasId, rHasId;
        static bool? rIsJoinedPair;
        public TL l;
        public TR r;

        static void ensureReflectedData(DataSource src)
        {
            var lTable = src.Table<TL>();
            if (lFieldsInfo == null)
                lTable.GetFieldsArraysWithId(out lFieldsInfo, out lFieldsNames, out lHasId);

            if (!rIsJoinedPair.HasValue) {
                rIsJoinedPair = false;
                if (typeof(TR).IsGenericType)
                    rIsJoinedPair = typeof(TR).GetGenericTypeDefinition() == typeof(JoinedRecord<,>);
                if (!rIsJoinedPair.Value) {
                    var rTable = src.Table<TR>();
                    rTable.GetFieldsArraysWithId(out rFieldsInfo, out rFieldsNames, out rHasId);
                }
            }
        }

        internal static JoinedRecord<TL, TR> readerToLine(DataSource src, IDataReader r, int startColumnIndex)
        {
            ensureReflectedData(src);
            var lTable = src.Table<TL>();
            var result = new JoinedRecord<TL, TR> {l = lTable.readerToLine(r, startColumnIndex)};
            if (rIsJoinedPair != null && rIsJoinedPair.Value) {
                var cType = typeof(TR);
                var mthd = cType.GetMethod("readerToLine", BindingFlags.Static);
                if (mthd != null)
                    result.r = (TR) mthd.Invoke(null, new object[] {src, r, startColumnIndex + lFieldsInfo.Length});
            } else {
                var cTable = src.Table<TR>();
                result.r = cTable.readerToLine(r, startColumnIndex + lFieldsNames.Length);
            }

            return result;
        }

        internal static string fieldsList(DataSource src)
        {
            ensureReflectedData(src);
            var part1 = src.Table<TL>().BuildSqlFieldsListWithTable();
            string part2 = null;
            if (rIsJoinedPair != null && rIsJoinedPair.Value) {
                var cType = typeof(TR);
                var c_filedsList_mthd = cType.GetMethod("fieldsList", BindingFlags.Static | BindingFlags.NonPublic);
                if (c_filedsList_mthd != null) part2 = (string) c_filedsList_mthd.Invoke(null, new object[] {src});
            } else {
                part2 = src.Table<TR>().BuildSqlFieldsListWithTable();
            }

            return part1 + "," + part2;
        }

        static string LTableName(DataSource src)
        {
            ensureReflectedData(src);
            return src.Table<TL>().TableName;
        }

        static string wrapFromSection(string parentFromSection, DataSource src, List<string> onChildFields,
            int atHirarchyIndex)
        {
            var rightTable = getRightTable(src);
            var sb = new StringBuilder();
            sb.Append("(" + parentFromSection + ")");
            sb.Append(" LEFT JOIN " + rightTable);
            sb.Append(" ON ");
            sb.Append(src.Table<TL>().TableName + "." + src.Table<TL>().IDField);
            sb.Append("=");
            sb.Append(rightTable + "." + onChildFields[atHirarchyIndex]);
            var result = sb.ToString();
            if (rIsJoinedPair != null && rIsJoinedPair.Value) {
                var c_wrapFromSection_mthd =
                    typeof(TR).GetMethod("wrapFromSection", BindingFlags.Static | BindingFlags.Static);
                if (c_wrapFromSection_mthd != null)
                    result = (string) c_wrapFromSection_mthd.Invoke(null,
                        new object[] {
                            result, src, onChildFields, atHirarchyIndex + 1
                        });
            }

            return result;
        }

        internal static string fromSection(DataSource src, List<string> onChildFields)
        {
            ensureReflectedData(src);
            /*
             * TableName + " LEFT JOIN " + cTable.TableName + " ON " +
                TableName + "." + IDField + " = " +
                cTable.TableName + "." + onChildField
             * */
            var leftTable = src.Table<TL>().TableName;
            var leftId = src.Table<TL>().IDField;

            var rightTable = getRightTable(src);

            var sb = new StringBuilder();
            sb.Append(leftTable + " LEFT JOIN " + rightTable);
            sb.Append(" ON ");
            sb.Append(leftTable + "." + leftId);
            sb.Append("=");
            sb.Append(rightTable + "." + onChildFields[0]);
            var result = sb.ToString();
            if (rIsJoinedPair != null && rIsJoinedPair.Value) {
                var c_wrapFromSection_mthd =
                    typeof(TR).GetMethod("wrapFromSection", BindingFlags.Static | BindingFlags.NonPublic);
                if (c_wrapFromSection_mthd != null)
                    result = (string) c_wrapFromSection_mthd.Invoke(null,
                        new object[] {
                            result, src, onChildFields, 1
                        });
            }

            return result;
        }

        static string getRightTable(DataSource src)
        {
            string rightTable = null;
            if (rIsJoinedPair != null && rIsJoinedPair.Value) {
                var c_PTableName_mthd = typeof(TR).GetMethod("LTableName", BindingFlags.NonPublic | BindingFlags.Static);
                if (c_PTableName_mthd != null) rightTable = (string) c_PTableName_mthd.Invoke(null, new object[] {src});
            } else {
                rightTable = src.Table<TR>().TableName;
            }

            return rightTable;
        }
    }

    public class JoinedRecordSet<P, C> : QueryNonTableSet<JoinedRecord<P, C>>
        where P : class, new()
        where C : class, new()
    {
        readonly List<string>
            onChildFields; // a list, where the first index is the onField of C child, and the last is the last hierarchy child C of C


        internal JoinedRecordSet(DataSource src, IEnumerable<string> onChildFields, string whereSection,
            string orderBy) :
            base(src, false, null, whereSection, orderBy)
        {
            this.onChildFields = new List<string>(onChildFields);
        }

        protected override string FromSection => JoinedRecord<P, C>.fromSection(Src, onChildFields);

        protected override string SelectFields => JoinedRecord<P, C>.fieldsList(Src);

        public override IQuerySet<JoinedRecord<P, C>> Sort(string fieldsList) =>
            new JoinedRecordSet<P, C>(Src, onChildFields, WhereSection, fieldsList);

        public override IQuerySet<JoinedRecord<P, C>> Subset(string moreCriteria) =>
            new JoinedRecordSet<P, C>(Src, onChildFields, GetWhereSection(moreCriteria), OrderBySection);

        /*
        public JoinedRecordSet<P, JoinedRecord<C, J>> Join<J>(string onField)
            where J: class, new()
        {
            List<string> joinedOnChildFields = new List<string>(onChildFields);
            joinedOnChildFields.Add(onField);

            return new JoinedRecordSet<P,JoinedRecord<C,J>>(
                src, joinedOnChildFields, whereSection, orderBySection);
        }*/
    }
}