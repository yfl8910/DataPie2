using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace DBUtil
{
    // The caller owns the source reader. Normalize only columns whose destination is typed.
    internal sealed class ImportDataReader : IDataReader
    {
        private readonly IDataReader source;
        private readonly bool[] normalize;

        public ImportDataReader(IDataReader source, IReadOnlyDictionary<string, Type> columns)
        {
            this.source = source;
            normalize = Enumerable.Range(0, source.FieldCount)
                .Select(i => columns.TryGetValue(source.GetName(i), out var type) && IsTypedColumn(type)).ToArray();
        }

        private static bool IsTypedColumn(Type type)
        {
            type = Nullable.GetUnderlyingType(type) ?? type;
            return type == typeof(TimeSpan) || type == typeof(DateTimeOffset) ||
                Type.GetTypeCode(type) is TypeCode.Boolean or TypeCode.DateTime or
                TypeCode.Byte or TypeCode.SByte or TypeCode.Int16 or TypeCode.UInt16 or
                TypeCode.Int32 or TypeCode.UInt32 or TypeCode.Int64 or TypeCode.UInt64 or
                TypeCode.Single or TypeCode.Double or TypeCode.Decimal;
        }

        public object GetValue(int i)
        {
            var value = source.GetValue(i);
            return value == null || (normalize[i] && value is string text && string.IsNullOrWhiteSpace(text))
                ? DBNull.Value : value;
        }
        public bool IsDBNull(int i) => GetValue(i) is DBNull;
        public object this[int i] => GetValue(i);
        public object this[string name] => GetValue(GetOrdinal(name));
        public int GetValues(object[] values)
        {
            int count = Math.Min(values.Length, FieldCount);
            for (int i = 0; i < count; i++) values[i] = GetValue(i);
            return count;
        }
        public int FieldCount => source.FieldCount;
        public int Depth => source.Depth;
        public bool IsClosed => source.IsClosed;
        public int RecordsAffected => source.RecordsAffected;
        public bool Read() => source.Read();
        public bool NextResult() => source.NextResult();
        public void Close() { }
        public void Dispose() { }
        public string GetName(int i) => source.GetName(i);
        public int GetOrdinal(string name) => source.GetOrdinal(name);
        public Type GetFieldType(int i) => source.GetFieldType(i);
        public string GetDataTypeName(int i) => source.GetDataTypeName(i);
        public DataTable GetSchemaTable() => source.GetSchemaTable();
        public IDataReader GetData(int i) => source.GetData(i);
        public bool GetBoolean(int i) => source.GetBoolean(i);
        public byte GetByte(int i) => source.GetByte(i);
        public char GetChar(int i) => source.GetChar(i);
        public DateTime GetDateTime(int i) => source.GetDateTime(i);
        public decimal GetDecimal(int i) => source.GetDecimal(i);
        public double GetDouble(int i) => source.GetDouble(i);
        public float GetFloat(int i) => source.GetFloat(i);
        public Guid GetGuid(int i) => source.GetGuid(i);
        public short GetInt16(int i) => source.GetInt16(i);
        public int GetInt32(int i) => source.GetInt32(i);
        public long GetInt64(int i) => source.GetInt64(i);
        public string GetString(int i) => source.GetString(i);
        public long GetBytes(int i, long offset, byte[] buffer, int bufferOffset, int length)
            => source.GetBytes(i, offset, buffer, bufferOffset, length);
        public long GetChars(int i, long offset, char[] buffer, int bufferOffset, int length)
            => source.GetChars(i, offset, buffer, bufferOffset, length);
    }
}
