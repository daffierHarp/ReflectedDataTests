
namespace ReflectedData
{
    // TODO: add these functions
    // sum, count(criteria)
    // TODO: extract class SelectSet

    /// <summary>
    ///     On a record that is mapped to a relation, define a field as JoinSet instead of list
    ///     and set the DataJoinToMany to have access on demand to child records in a "join/parent-child" relationship
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class JoinSet<T> : QueryTableSet<T>
        where T : class, new()
    {
        readonly string _onField;
        readonly object _onId;

        internal JoinSet(string onField, object onId, ReflectedTable<T> table) : base(null, null, table)
        {
            this._onId = onId;
            this._onField = onField;
        }

        protected override string GetCriteria() => _onField + "=" + DataSource.ValueToSql(_onId);


        public override void Add(T item)
        {
            var fi = Table.GetFieldInfoFromTableField(_onField);
            fi.SetValue(item, _onId);
            Table.Insert(item);
        }

        public override void Clear()
        {
            Table.MyDataSource.Delete(Table.TableName, _onField, _onId);
        }

        public override void CopyTo(T[] array, int arrayIndex)
        {
            var list = Table.ByField(_onField, _onId);
            list.CopyTo(array, arrayIndex);
        }
    }
}