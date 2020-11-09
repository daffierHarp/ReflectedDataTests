
#region using

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Reflection;

#endregion

namespace ReflectedData
{
    internal class DataReaderNonTableEnumerator<T> : IEnumerator<T> where T : class, new()
    {
        List<FieldInfo> _fields;
        readonly bool _isJoinedRecord;
        readonly MethodInfo _joinedRecordFromReaderMethod;
        IDataReader _r;
        DataSource _src;

        internal DataReaderNonTableEnumerator(DataSource src, List<FieldInfo> fields, IDataReader r)
        {
            this._src = src;
            this._r = r;
            this._fields = fields;
            if (typeof(T).IsGenericType)
                _isJoinedRecord = typeof(T).GetGenericTypeDefinition() == typeof(JoinedRecord<,>);
            if (_isJoinedRecord)
            {
                _joinedRecordFromReaderMethod =
                    typeof(T).GetMethod("ReaderToLine", BindingFlags.Static | BindingFlags.Public);
                if (_joinedRecordFromReaderMethod == null)
                    _joinedRecordFromReaderMethod =
                        typeof(T).GetMethod("readerToLine", BindingFlags.Static | BindingFlags.NonPublic);
            }
        }


        #region IEnumerator<T> Members

        public T Current
        {
            get
            {
                if (_isJoinedRecord)
                    return (T) _joinedRecordFromReaderMethod.Invoke(null,
                        new object[] {_src, _r, 0});
                return DataSource.readerToLine<T>(_fields, _r);
            }
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            _src.DisposeReader(_r);
            _src = null;
            _r = null;
            _fields = null;
        }

        #endregion

        #region IEnumerator Members

        object IEnumerator.Current => Current;

        public bool MoveNext() => _r.Read();

        public void Reset()
        {
            throw new NotSupportedException();
        }

        #endregion
    }

    internal class QueryNonTableEnumerable<T> : IEnumerable<T> where T : class, new()
    {
        readonly List<FieldInfo> _fields;
        readonly string _sql;
        readonly DataSource _src;

        internal QueryNonTableEnumerable(DataSource src, List<FieldInfo> fields, string sql)
        {
            this._src = src;
            this._fields = fields;
            this._sql = sql;
        }

        #region IEnumerable<T> Members

        public IEnumerator<T> GetEnumerator()
        {
            var r = _src.ExecuteReader(_sql);
            return new DataReaderNonTableEnumerator<T>(_src, _fields, r);
        }

        #endregion

        #region IEnumerable Members

        IEnumerator IEnumerable.GetEnumerator()
        {
            var r = _src.ExecuteReader(_sql);
            return new DataReaderNonTableEnumerator<T>(_src, _fields, r);
        }

        #endregion
    }

    internal class DataTableReaderEnumerator<T> : IEnumerator<T> where T : class, new()
    {
        IDataReader _r;
        DataSource _src;
        ReflectedTable<T> _table;

        internal DataTableReaderEnumerator(DataSource src, ReflectedTable<T> table, IDataReader r)
        {
            this._src = src;
            this._r = r;
            this._table = table;
        }


        #region IEnumerator<T> Members

        public T Current => _table.ReaderToLine(_r);

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            _src.DisposeReader(_r);
            _src = null;
            _r = null;
            _table = null;
        }

        #endregion

        #region IEnumerator Members

        object IEnumerator.Current => _table.ReaderToLine(_r);

        public bool MoveNext() => _r.Read();

        public void Reset()
        {
            throw new NotSupportedException();
        }

        #endregion
    }

    internal class QueryTableEnumerable<T> : IEnumerable<T> where T : class, new()
    {
        readonly string _sql;
        readonly ReflectedTable<T> _table;

        internal QueryTableEnumerable(ReflectedTable<T> table, string sql)
        {
            this._table = table;
            this._sql = sql;
        }

        #region IEnumerable<T> Members

        public IEnumerator<T> GetEnumerator()
        {
            var r = _table.MyDataSource.ExecuteReader(_sql);
            return new DataTableReaderEnumerator<T>(_table.MyDataSource, _table, r);
        }

        #endregion

        #region IEnumerable Members

        IEnumerator IEnumerable.GetEnumerator()
        {
            var r = _table.MyDataSource.ExecuteReader(_sql);
            return new DataTableReaderEnumerator<T>(_table.MyDataSource, _table, r);
        }

        #endregion
    }
}