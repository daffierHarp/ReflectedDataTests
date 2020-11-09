
#region using

using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
// ReSharper disable UnusedMember.Global

#endregion

// ReSharper disable once CheckNamespace
namespace ReflectedData
{
    /// <summary>
    ///     Lines in a reflected table do not have to derive from ReflectedTableLine, but it can help
    ///     for advanced joined connections between tables
    /// </summary>
    public class ReflectedTableLine
    {
        static Dictionary<Type, List<FieldInfo>> _cacheTypeToJoinSetFields;

        /// <summary>
        ///     to access table, call AtTable as ReflectedTable of this line's type
        /// </summary>
        [DataField(Ignore = true)] public object AtTable = null;

        [DataField(Ignore = true)] public bool DataConnected = false;

        /// <summary>
        ///     maintain index field copy when there is no ID field so that auto update can be set to
        ///     the right line if the index field changes
        /// </summary>
        internal string IndexCopy = null;

        [DataField(Ignore = true)] public bool IsDeleted = false;

        [DataField(Ignore = true)] public DataSource Source = null;
        /// <summary>
        /// Dictionary from instance-field name to last data-base value in support of smart updates which do not modify values that aren't changed since last retrieval
        /// </summary>
        // ReSharper disable once InconsistentNaming
        [DataField(Ignore = true)] internal Dictionary<string, object> __smartUpdate_LastValues;

        public string IndexFieldCopy => IndexCopy;

        /// <summary>
        /// check "smart-update" values of record which were cached when the record was retrieved from the database for the value of a field. 
        /// </summary>
        /// <param name="fieldName">The class field name (not the database renamed field name)</param>
        /// <returns>null if missing cache, false if had not changed, true if did. Throws exception if field name does does reflect in current type</returns>
        public bool? DidFieldChange(string fieldName)
        {
            if (__smartUpdate_LastValues == null || !__smartUpdate_LastValues.ContainsKey(fieldName)) return null;
            var fi = GetType().GetField(fieldName);
            if (fi == null) throw new Exception("Field name does not exist");
            var v = fi.GetValue(this);
            if (Equals(v, __smartUpdate_LastValues[fieldName])) return false;
            return true;
        }

        /// <summary>
        ///     get instances of parent/child as defined in attributes, will skip fields where the value is not null
        /// </summary>
        public void FillJoins()
        {
            if (!DataConnected)
                throw new Exception("Data was not retrieved from table");
            var ttype = GetType();
            foreach (var f in ttype.GetFields(BindingFlags.Public | BindingFlags.Instance |
                                              BindingFlags.FlattenHierarchy))
                fillJoin(f);
        }

        public bool InstantiateJoinSets()
        {
            if (!DataConnected)
                throw new Exception("Data was not retrieved from table");
            var ttype = GetType();
            if (_cacheTypeToJoinSetFields == null)
                _cacheTypeToJoinSetFields = new Dictionary<Type, List<FieldInfo>>();
            if (!_cacheTypeToJoinSetFields.ContainsKey(ttype)) {
                var joinSetFields = new List<FieldInfo>();
                foreach (var f in ttype.GetFields(BindingFlags.Public | BindingFlags.Instance |
                                                  BindingFlags.FlattenHierarchy))
                    if (f.FieldType.IsGenericType &&
                        f.FieldType.GetGenericTypeDefinition() == typeof(JoinSet<>))
                        joinSetFields.Add(f);
                _cacheTypeToJoinSetFields.Add(ttype, joinSetFields.Count > 0 ? joinSetFields : null);
            }

            if (_cacheTypeToJoinSetFields[ttype] == null)
                return false;

            foreach (var f in _cacheTypeToJoinSetFields[ttype])
                fillJoin(f);
            return true;
        }

        public void FillJoin(string fieldName)
        {
            var ttype = GetType();
            fillJoin(ttype.GetField(fieldName));
        }

        void fillJoin(FieldInfo fi)
        {
            if (fi == null) return;
            var attributes = fi.GetCustomAttributes(typeof(DataFieldAttribute), true);
            if (attributes.Length == 0) return;
            var attr = attributes[0] as DataFieldAttribute;

            if (attr is DataJoinToOne dj1) {
                if (fi.GetValue(this) != null) // joins will be filled only on fields where the value is null
                    return;
                fillJoinToOne(fi, dj1);
                return;
            }

            if (attr is DataJoinToMany djMany) {
                if (fi.GetValue(this) != null) // joins will be filled only on fields where the value is null
                    return;
                fillJoinToMany(fi, djMany);
            }
        }

        void fillJoinToMany(FieldInfo f, DataJoinToMany dataJoinToMany)
        {
            // getting types
            var fieldType = f.FieldType;
            var lineType = fieldType.GetGenericArguments()[0];
            var tblInstance = Source.Tables.Get(lineType);
            var isSet = false;
            if (fieldType.GetGenericTypeDefinition() != typeof(List<>)) {
                if (fieldType.GetGenericTypeDefinition() == typeof(JoinSet<>))
                    isSet = true;
                else throw new NotSupportedException();
            }

            var tableOfLineType = tblInstance.GetType();
            // deciding on this field, which could be declared, or would be the id field of this line's table
            var thisField = dataJoinToMany.ThisFieldName;
            FieldInfo thisFieldInfo;
            if (thisField != null) {
                thisFieldInfo = GetType().GetField(thisField);
            } else {
                var thisLineTableType = AtTable.GetType();
                thisFieldInfo = (FieldInfo) thisLineTableType.GetField("idFieldInfo", BindingFlags.Instance |
                                                                                      BindingFlags.NonPublic |
                                                                                      BindingFlags.Public)
                    ?.GetValue(AtTable);
                if (thisFieldInfo != null) thisField = thisFieldInfo.Name;
            }

            // get ready to query
            if (thisFieldInfo != null) {
                var lookupValue = thisFieldInfo.GetValue(this);
                var otherTableField = dataJoinToMany.OtherTableField;
                var thisFieldName = thisField; // this field's table name
                var thisFieldAttrs = thisFieldInfo.GetCustomAttributes(typeof(DataFieldAttribute), true);
                // if the name of this line's field is renamed, get the renamed
                if (thisFieldAttrs.Length > 0) {
                    if (thisFieldAttrs[0] is DataFieldAttribute thisFieldAttr && thisFieldAttr.Rename != null)
                        thisFieldName = thisFieldAttr.Rename;
                }

                // when not declared the field name at the joined table, assume it's the same as id of this line
                if (otherTableField == null)
                    otherTableField = thisFieldName;

                if (isSet) {
                    /*object joinSetInstance = Activator.CreateInstance(
                    fieldType, otherTableField, lookupValue, tblInstance);*/

                    var byFieldSetMethod = tableOfLineType.GetMethod("ByFieldSet", new[] {typeof(string), typeof(object)});
                    if (byFieldSetMethod != null) {
                        var joinSetInstance = byFieldSetMethod.Invoke(tblInstance, new[] {otherTableField, lookupValue});
                        f.SetValue(this, joinSetInstance);
                    }

                    return;
                }

                // using reflected invoke - get the list
                var selMthd = tableOfLineType.GetMethod("ByField", new[] {typeof(string), typeof(object)});
                if (selMthd != null) {
                    var joinedList = selMthd.Invoke(tblInstance, new[] {otherTableField, lookupValue});

                    // if the lines of the joined table are also reflected-table-line and are joined to one on the other
                    // table field - set instance to this (strong handshake between one to many instances)
                    if (typeof(ReflectedTableLine).IsAssignableFrom(lineType)) {
                        FieldInfo filedInfoJoinedToThis = null;
                        foreach (var fi in lineType.GetFields()) {
                            if (!GetType().IsAssignableFrom(fi.FieldType))
                                continue;

                            var attributes = fi.GetCustomAttributes(typeof(DataJoinToOne), true);
                            if (attributes.Length == 0) continue;
                            if (attributes[0] is DataJoinToOne dataJoinToOne && (dataJoinToOne.OtherTableField == thisFieldName ||
                                                                                 dataJoinToOne.OtherTableField == null && dataJoinToOne.ThisFieldName == thisFieldName)) {
                                filedInfoJoinedToThis = fi;
                                break;
                            }
                        }

                        if (filedInfoJoinedToThis != null)
                            foreach (var resultLine in (IEnumerable) joinedList)
                                filedInfoJoinedToThis.SetValue(resultLine, this);
                    }

                    f.SetValue(this, joinedList);
                }
            }
        }

        void fillJoinToOne(FieldInfo f, DataJoinToOne attr)
        {
            var fieldType = f.FieldType;
            var tblInstance = Source.Tables.Get(fieldType);
            var tableOfFieldType = tblInstance.GetType();

            var joinedIdField = attr.OtherTableField ?? (string) tableOfFieldType.GetField("IDField").GetValue(tblInstance);
            var lookupValue = GetType().GetField(attr.ThisFieldName).GetValue(this);
            var selFirstMthd = tableOfFieldType.GetMethod("ByFieldFirst");
            if (selFirstMthd != null) {
                var joinedLine = selFirstMthd.Invoke(tblInstance, new[] {joinedIdField, lookupValue});
                f.SetValue(this, joinedLine);
            }
        }

        /// <summary>
        ///     If this record is DataConnected, save any changes
        /// </summary>
        public void Update()
        {
            if (IsDeleted)
                throw new InvalidOperationException("Cannot update a deleted record");
            if (!DataConnected)
                throw new InvalidOperationException("New records need to be inserted, not updated");
            var thisLineTableType = AtTable.GetType();
            var updateMthd = thisLineTableType.GetMethod("Update");
            updateMthd?.Invoke(AtTable, new object[] {this});
        }

        public void UpdateOrInsert()
        {
            if (IsDeleted)
                throw new InvalidOperationException("Cannot update a deleted record");
            var thisLineTableType = AtTable.GetType();
            if (!DataConnected) {
                var inesrtMi = thisLineTableType.GetMethod("Insert");
                inesrtMi?.Invoke(AtTable, new object[] {this});
                return;
            }
            var updateMthd = thisLineTableType.GetMethod("Update");
            updateMthd?.Invoke(AtTable, new object[] {this});
        }

        public void Delete()
        {
            if (!DataConnected)
                throw new InvalidOperationException("Cannot delete record which was not read from data source");

            var thisLineTableType = AtTable.GetType();
            var deleteMthd = thisLineTableType.GetMethod("Delete", new[] {GetType()});
            deleteMthd?.Invoke(AtTable, new object[] {this});
        }
    }

    public class ReflectedTableLine<T> : ReflectedTableLine where T : class, new()
    {
        public new ReflectedTable<T> AtTable => (ReflectedTable<T>) base.AtTable;
    }
}