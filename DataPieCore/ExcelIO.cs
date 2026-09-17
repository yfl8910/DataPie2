using DBUtil;
using ExcelDataReader;
using MiniExcelLibs;
using MiniExcelLibs.OpenXml;
using OfficeOpenXml;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace DataPieCore
{
    public static class ExcelIO
    {
        public static void ExcelDataReaderImport(string filePath, string tableName, IDbAccess dbAccess)
        {
            ArgumentNullException.ThrowIfNull(filePath);
            ArgumentNullException.ThrowIfNull(dbAccess);
            using var stream = OpenReadStream(filePath);
            using var reader = ExcelReaderFactory.CreateReader(stream);
            SelectWorksheet(reader, tableName);
            using var headerReader = new HeaderRowDataReader(reader);
            dbAccess.BulkInsert(tableName, headerReader);
        }

        public static void MiniExcelReaderImport(string filePath, string tableName, IDbAccess dbAccess)
        {
            ArgumentNullException.ThrowIfNull(filePath);
            ArgumentNullException.ThrowIfNull(dbAccess);
            using var stream = OpenReadStream(filePath);
            using var reader = MiniExcel.GetReader(stream, true, sheetName: tableName);
            dbAccess.BulkInsert(tableName, reader);
        }

        public static void CsvImport(string filePath, string tableName, IDbAccess dbAccess)
        {
            if (filePath is null) throw new ArgumentNullException(nameof(filePath));
            if (dbAccess is null) throw new ArgumentNullException(nameof(dbAccess));

            using var stream = OpenReadStream(filePath);
            using var reader = CreateCsvReader(stream);
            using var headerReader = new HeaderRowDataReader(reader);
            dbAccess.BulkInsert(tableName, headerReader);
        }

        public static int SaveExcel(string filePath, IDataReader reader, string sheetName)
        {
            if (filePath is null) throw new ArgumentNullException(nameof(filePath));
            if (reader is null) throw new ArgumentNullException(nameof(reader));

            var watch = Stopwatch.StartNew();

            ExcelPackage.License.SetNonCommercialOrganization("<DataPie>");

            PrepareOutputFile(filePath);

            using (var package = new ExcelPackage(new FileInfo(filePath)))
            {
                var ws = package.Workbook.Worksheets.Add(sheetName);
                ws.Cells["A1"].LoadFromDataReader(reader, true);
                package.Save();
            }

            watch.Stop();
            return (int)watch.Elapsed.TotalSeconds;
        }

        public static int ExportSheetsWithEpplus(IList<string> tableNames, string filePath, IDbAccess dbAccess, string databaseType)
        {
            if (filePath is null) throw new ArgumentNullException(nameof(filePath));
            if (tableNames is null || tableNames.Count == 0) throw new ArgumentException("tableNames required", nameof(tableNames));
            if (dbAccess is null) throw new ArgumentNullException(nameof(dbAccess));

            var watch = Stopwatch.StartNew();

            // Keep EPPlus licensing call if required by your usage
            ExcelPackage.License.SetNonCommercialOrganization("<DataPie>");

            PrepareOutputFile(filePath);

            using (var package = new ExcelPackage(new FileInfo(filePath)))
            {
                foreach (var table in tableNames)
                {
                    string sql = SqlQueryBuilder.BuildSelectAll(table, databaseType);
                    using (var reader = dbAccess.GetDataReader(sql))
                    {
                        var ws = package.Workbook.Worksheets.Add(table);
                        ws.Cells["A1"].LoadFromDataReader(reader, true);
                    }
                }

                package.Save();
            }

            watch.Stop();
            return (int)watch.Elapsed.TotalSeconds;
        }
        public static int ExportSheetsWithMiniExcel(IList<string> tableNames, string filePath, IDbAccess dbAccess, string databaseType)
        {
            if (filePath is null) throw new ArgumentNullException(nameof(filePath));
            if (tableNames is null || tableNames.Count == 0) throw new ArgumentException("tableNames required", nameof(tableNames));
            if (dbAccess is null) throw new ArgumentNullException(nameof(dbAccess));

            var watch = Stopwatch.StartNew();

            PrepareOutputFile(filePath);

            var sheets = new Dictionary<string, object>();

            var readers = new List<IDataReader>();

            try
            {
                foreach (var table in tableNames)
                {
                    string sql = SqlQueryBuilder.BuildSelectAll(table, databaseType);
                    var reader = new DeferredDataReader(dbAccess.CreateNewAccess, sql);
                    readers.Add(reader);
                    sheets.Add(table, reader);
                }

                MiniExcel.SaveAs(filePath, sheets, printHeader: true, configuration: CreateExportConfiguration());
            }
            finally
            {
                foreach (var reader in readers)
                {
                    reader.Dispose();
                }
            }

            watch.Stop();
            return (int)watch.Elapsed.TotalSeconds;
        }

        public static int SaveMiniExcel(string filePath, IDataReader reader, string sheetName)
        {
            if (filePath is null) throw new ArgumentNullException(nameof(filePath));
            if (reader is null) throw new ArgumentNullException(nameof(reader));

            var watch = Stopwatch.StartNew();

            PrepareOutputFile(filePath);

            MiniExcel.SaveAs(filePath, reader, printHeader: true, sheetName: sheetName, configuration: CreateExportConfiguration());

            watch.Stop();
            return (int)watch.Elapsed.TotalSeconds;
        }

        private static void PrepareOutputFile(string filePath)
        {
            if (File.Exists(filePath)) File.Delete(filePath);
        }

        private static OpenXmlConfiguration CreateExportConfiguration()
            => new OpenXmlConfiguration { TableStyles = TableStyles.None };

        private static FileStream OpenReadStream(string filePath)
        {
            return new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.SequentialScan);
        }

        private static void SelectWorksheet(IExcelDataReader reader, string sheetName)
        {
            if (string.IsNullOrWhiteSpace(sheetName)) return;
            do
            {
                if (reader.Name == sheetName) return;
            }
            while (reader.NextResult());
            throw new ArgumentException($"Worksheet '{sheetName}' was not found.", nameof(sheetName));
        }

        private static IExcelDataReader CreateCsvReader(Stream stream)
        {
            return ExcelReaderFactory.CreateCsvReader(stream, new ExcelReaderConfiguration
            {
                FallbackEncoding = Encoding.GetEncoding("GB2312"),
                AutodetectSeparators = new[] { ',', ';', '\t', '|', '#' },
            });
        }

        private sealed class HeaderRowDataReader : IDataReader
        {
            private readonly IDataReader innerReader;
            private readonly string[] fieldNames;

            public HeaderRowDataReader(IDataReader innerReader)
            {
                this.innerReader = innerReader ?? throw new ArgumentNullException(nameof(innerReader));
                fieldNames = new string[innerReader.FieldCount];

                if (!innerReader.Read())
                {
                    return;
                }

                for (int i = 0; i < fieldNames.Length; i++)
                {
                    var name = innerReader.GetValue(i)?.ToString();
                    fieldNames[i] = string.IsNullOrWhiteSpace(name) ? $"Column{i + 1}" : name;
                }
            }

            public int FieldCount => innerReader.FieldCount;
            public object this[int i] => innerReader[i];
            public object this[string name] => innerReader[GetOrdinal(name)];
            public int Depth => innerReader.Depth;
            public bool IsClosed => innerReader.IsClosed;
            public int RecordsAffected => innerReader.RecordsAffected;

            public void Close() => innerReader.Close();
            public void Dispose() => innerReader.Dispose();
            public bool GetBoolean(int i) => innerReader.GetBoolean(i);
            public byte GetByte(int i) => innerReader.GetByte(i);
            public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => innerReader.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
            public char GetChar(int i) => innerReader.GetChar(i);
            public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => innerReader.GetChars(i, fieldoffset, buffer, bufferoffset, length);
            public IDataReader GetData(int i) => innerReader.GetData(i);
            public string GetDataTypeName(int i) => innerReader.GetDataTypeName(i);
            public DateTime GetDateTime(int i) => innerReader.GetDateTime(i);
            public decimal GetDecimal(int i) => innerReader.GetDecimal(i);
            public double GetDouble(int i) => innerReader.GetDouble(i);
            public Type GetFieldType(int i) => innerReader.GetFieldType(i);
            public float GetFloat(int i) => innerReader.GetFloat(i);
            public Guid GetGuid(int i) => innerReader.GetGuid(i);
            public short GetInt16(int i) => innerReader.GetInt16(i);
            public int GetInt32(int i) => innerReader.GetInt32(i);
            public long GetInt64(int i) => innerReader.GetInt64(i);
            public string GetName(int i) => fieldNames[i];
            public int GetOrdinal(string name) => Array.FindIndex(fieldNames, fieldName => string.Equals(fieldName, name, StringComparison.OrdinalIgnoreCase));
            public DataTable GetSchemaTable() => innerReader.GetSchemaTable();
            public string GetString(int i) => innerReader.GetString(i);
            public object GetValue(int i) => innerReader.GetValue(i);
            public int GetValues(object[] values) => innerReader.GetValues(values);
            public bool IsDBNull(int i) => innerReader.IsDBNull(i);
            public bool NextResult() => innerReader.NextResult();
            public bool Read() => innerReader.Read();
        }
    }
}
