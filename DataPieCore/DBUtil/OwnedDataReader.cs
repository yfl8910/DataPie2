using System;
using System.Data;

namespace DBUtil
{
    // IDataReader does not own its command by default. Keep their lifetimes together.
    internal sealed class OwnedDataReader : IDataReader
    {
        private readonly IDataReader reader;
        private readonly IDbCommand command;
        private readonly IDbAccess access;
        private bool disposed;

        private OwnedDataReader(IDataReader reader, IDbCommand command, IDbAccess access)
        {
            this.reader = reader;
            this.command = command;
            this.access = access;
        }

        public static IDataReader Open(IDbAccess access, string sql, IDbDataParameter[] parameters, int timeout)
        {
            var command = access.conn.CreateCommand();
            try
            {
                command.CommandText = sql;
                command.CommandTimeout = timeout;
                if (parameters != null)
                    foreach (var parameter in parameters) command.Parameters.Add(parameter);
                if (access.conn.State != ConnectionState.Open) access.conn.Open();
                access.IsOpen = true;
                return new OwnedDataReader(command.ExecuteReader(), command, access);
            }
            catch
            {
                try { command.Parameters.Clear(); command.Dispose(); }
                finally { ReleaseConnection(access); }
                throw;
            }
        }

        private static void ReleaseConnection(IDbAccess access)
        {
            if (!access.IsKeepConnect) access.conn.Close();
            access.IsOpen = access.conn.State == ConnectionState.Open;
        }

        public void Dispose()
        {
            if (disposed) return;
            disposed = true;
            try { reader.Dispose(); }
            finally
            {
                try { command.Parameters.Clear(); command.Dispose(); }
                finally { ReleaseConnection(access); }
            }
        }
        public void Close() => Dispose();
        public bool IsClosed => disposed || reader.IsClosed;
        public int FieldCount => reader.FieldCount;
        public int Depth => reader.Depth;
        public int RecordsAffected => reader.RecordsAffected;
        public object this[int i] => reader[i];
        public object this[string name] => reader[name];
        public bool Read() => reader.Read();
        public bool NextResult() => reader.NextResult();
        public string GetName(int i) => reader.GetName(i);
        public Type GetFieldType(int i) => reader.GetFieldType(i);
        public int GetOrdinal(string name) => reader.GetOrdinal(name);
        public bool GetBoolean(int i) => reader.GetBoolean(i);
        public byte GetByte(int i) => reader.GetByte(i);
        public long GetBytes(int i, long offset, byte[] buffer, int bufferOffset, int length) => reader.GetBytes(i, offset, buffer, bufferOffset, length);
        public char GetChar(int i) => reader.GetChar(i);
        public long GetChars(int i, long offset, char[] buffer, int bufferOffset, int length) => reader.GetChars(i, offset, buffer, bufferOffset, length);
        public IDataReader GetData(int i) => reader.GetData(i);
        public string GetDataTypeName(int i) => reader.GetDataTypeName(i);
        public DateTime GetDateTime(int i) => reader.GetDateTime(i);
        public decimal GetDecimal(int i) => reader.GetDecimal(i);
        public double GetDouble(int i) => reader.GetDouble(i);
        public float GetFloat(int i) => reader.GetFloat(i);
        public Guid GetGuid(int i) => reader.GetGuid(i);
        public short GetInt16(int i) => reader.GetInt16(i);
        public int GetInt32(int i) => reader.GetInt32(i);
        public long GetInt64(int i) => reader.GetInt64(i);
        public DataTable GetSchemaTable() => reader.GetSchemaTable();
        public string GetString(int i) => reader.GetString(i);
        public object GetValue(int i) => reader.GetValue(i);
        public int GetValues(object[] values) => reader.GetValues(values);
        public bool IsDBNull(int i) => reader.IsDBNull(i);
    }
}
