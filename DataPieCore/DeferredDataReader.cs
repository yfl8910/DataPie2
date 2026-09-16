using System;
using System.Data;
using DBUtil;

namespace DataPieCore
{
    // Opening is deferred until the writer reaches this sheet. Exhaustion releases
    // both the reader and its connection, including empty result sets.
    internal sealed class DeferredDataReader : IDataReader
    {
        private readonly Func<IDbAccess> createAccess;
        private readonly string sql;
        private IDbAccess access;
        private IDataReader reader;
        private string[] names;
        private Type[] types;
        private bool closed;

        public DeferredDataReader(Func<IDbAccess> createAccess, string sql)
        {
            this.createAccess = createAccess;
            this.sql = sql;
        }

        private IDataReader Reader
        {
            get
            {
                if (closed) throw new ObjectDisposedException(nameof(DeferredDataReader));
                if (reader != null) return reader;
                try
                {
                    access = createAccess();
                    reader = access.GetDataReader(sql);
                    names = new string[reader.FieldCount];
                    types = new Type[reader.FieldCount];
                    for (int i = 0; i < names.Length; i++)
                    {
                        names[i] = reader.GetName(i);
                        types[i] = reader.GetFieldType(i);
                    }
                    return reader;
                }
                catch { Dispose(); throw; }
            }
        }

        private void EnsureSchema() { if (names == null) _ = Reader; }
        public int FieldCount { get { EnsureSchema(); return names.Length; } }
        public string GetName(int i) { EnsureSchema(); return names[i]; }
        public Type GetFieldType(int i) { EnsureSchema(); return types[i]; }
        public int GetOrdinal(string name)
        {
            EnsureSchema();
            int ordinal = Array.FindIndex(names, n => string.Equals(n, name, StringComparison.OrdinalIgnoreCase));
            return ordinal >= 0 ? ordinal : throw new IndexOutOfRangeException(name);
        }
        public bool Read()
        {
            if (closed) return false;
            try
            {
                if (Reader.Read()) return true;
                Dispose();
                return false;
            }
            catch { Dispose(); throw; }
        }
        public void Dispose()
        {
            if (closed) return;
            closed = true;
            try { reader?.Dispose(); }
            finally { access?.Dispose(); }
        }
        public void Close() => Dispose();
        public bool IsClosed => closed;
        public bool NextResult() => false;
        public int Depth => 0;
        public int RecordsAffected => -1;
        public object this[int i] => Reader[i];
        public object this[string name] => Reader[GetOrdinal(name)];
        public bool GetBoolean(int i) => Reader.GetBoolean(i);
        public byte GetByte(int i) => Reader.GetByte(i);
        public long GetBytes(int i, long offset, byte[] buffer, int bufferOffset, int length) => Reader.GetBytes(i, offset, buffer, bufferOffset, length);
        public char GetChar(int i) => Reader.GetChar(i);
        public long GetChars(int i, long offset, char[] buffer, int bufferOffset, int length) => Reader.GetChars(i, offset, buffer, bufferOffset, length);
        public IDataReader GetData(int i) => Reader.GetData(i);
        public string GetDataTypeName(int i) => Reader.GetDataTypeName(i);
        public DateTime GetDateTime(int i) => Reader.GetDateTime(i);
        public decimal GetDecimal(int i) => Reader.GetDecimal(i);
        public double GetDouble(int i) => Reader.GetDouble(i);
        public float GetFloat(int i) => Reader.GetFloat(i);
        public Guid GetGuid(int i) => Reader.GetGuid(i);
        public short GetInt16(int i) => Reader.GetInt16(i);
        public int GetInt32(int i) => Reader.GetInt32(i);
        public long GetInt64(int i) => Reader.GetInt64(i);
        public DataTable GetSchemaTable() => Reader.GetSchemaTable();
        public string GetString(int i) => Reader.GetString(i);
        public object GetValue(int i) => Reader.GetValue(i);
        public int GetValues(object[] values) => Reader.GetValues(values);
        public bool IsDBNull(int i) => Reader.IsDBNull(i);
    }
}
