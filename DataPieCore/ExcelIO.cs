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
    public class ExcelIO
    {
        public static void ExcelDataReaderImport(string filePath, string tableName, IDbAccess dbAccess)
        {
            if (filePath is null) throw new ArgumentNullException(nameof(filePath));
            if (dbAccess is null) throw new ArgumentNullException(nameof(dbAccess));

            var reader = CreateExcelReader(filePath, tableName, out var stream);
            using (stream)
            using (reader)
            using (var headerReader = new HeaderRowDataReader(reader))
            {
                dbAccess.BulkInsert(tableName, headerReader);
            }
        }

        public static void MiniExcelReaderImport(string filePath, string tableName, IDbAccess dbAccess)
        {
            if (filePath is null) throw new ArgumentNullException(nameof(filePath));
            if (dbAccess is null) throw new ArgumentNullException(nameof(dbAccess));

            // Try to open a reader for the requested sheet first; if not found, fall back to first sheet.
            IDataReader reader = null;
            try
            {
                try
                {
                    reader = MiniExcel.GetReader(filePath, true, sheetName: tableName);
                }
                catch
                {
                    reader = MiniExcel.GetReader(filePath, true);
                }

                using (reader)
                {
                    dbAccess.BulkInsert(tableName, reader);
                }
            }
            finally
            {
                // ensured disposal via using above
            }
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

        public static void DataTableImport(DataTable dt, string tableName, IDbAccess dbAccess)
        {
            if (dt is null) throw new ArgumentNullException(nameof(dt));
            if (dbAccess is null) throw new ArgumentNullException(nameof(dbAccess));

            dbAccess.BulkInsert(tableName, dt);
        }

        public static int SaveExcel(string FileName, IDataReader reader, string SheetName)
        {
            if (FileName is null) throw new ArgumentNullException(nameof(FileName));
            if (reader is null) throw new ArgumentNullException(nameof(reader));

            var watch = Stopwatch.StartNew();

            ExcelPackage.LicenseContext = LicenseContext.NonCommercial;

            var newFile = new FileInfo(FileName);
            if (newFile.Exists)
            {
                newFile.Delete();
                newFile = new FileInfo(FileName);
            }

            using (var package = new ExcelPackage(newFile))
            {
                using (reader)
                {
                    var ws = package.Workbook.Worksheets.Add(SheetName);
                    ws.Cells["A1"].LoadFromDataReader(reader, true);
                }
                package.Save();
            }

            watch.Stop();
            return (int)watch.Elapsed.TotalSeconds;
        }

        public static int SaveMutiExcel(IList<string> tableNames, string filename, IDbAccess dbAccess, string dbtype)
        {
            if (filename is null) throw new ArgumentNullException(nameof(filename));
            if (tableNames is null || tableNames.Count == 0) throw new ArgumentException("tableNames required", nameof(tableNames));
            if (dbAccess is null) throw new ArgumentNullException(nameof(dbAccess));

            var watch = Stopwatch.StartNew();

            // Keep EPPlus licensing call if required by your usage
            ExcelPackage.License.SetNonCommercialOrganization("<DataPie>");

            var newFile = new FileInfo(filename);
            if (newFile.Exists)
            {
                newFile.Delete();
                newFile = new FileInfo(filename);
            }

            using (var package = new ExcelPackage(newFile))
            {
                foreach (var table in tableNames)
                {
                    string sql = BuildSQl.GetSQLfromTable(table, dbtype);
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
        public static int SaveMutiMiniExcel(IList<string> tableNames, string filename, IDbAccess dbAccess, string dbtype)
        {
            if (filename is null) throw new ArgumentNullException(nameof(filename));
            if (tableNames is null || tableNames.Count == 0) throw new ArgumentException("tableNames required", nameof(tableNames));
            if (dbAccess is null) throw new ArgumentNullException(nameof(dbAccess));

            var watch = Stopwatch.StartNew();

            var newFile = new FileInfo(filename);
            if (newFile.Exists)
            {
                newFile.Delete();
                newFile = new FileInfo(filename);
            }

            var sheets = new Dictionary<string, object>();

            var readers = new List<IDataReader>();

            try
            {
                foreach (var table in tableNames)
                {
                    string sql = BuildSQl.GetSQLfromTable(table, dbtype);
                    var reader = new DeferredDataReader(dbAccess.CreateNewIDB, sql);
                    readers.Add(reader);
                    sheets.Add(table, reader);
                }

                var config = new OpenXmlConfiguration()
                {
                    TableStyles = TableStyles.None
                };

                MiniExcel.SaveAs(newFile.ToString(), sheets, configuration: config);
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

        public static int SaveMiniExcel(string FileName, DataTable table, string SheetName)
        {
            if (FileName is null) throw new ArgumentNullException(nameof(FileName));
            if (table is null) throw new ArgumentNullException(nameof(table));

            var watch = Stopwatch.StartNew();

            var newFile = new FileInfo(FileName);
            if (newFile.Exists)
            {
                newFile.Delete();
                newFile = new FileInfo(FileName);
            }

            MiniExcel.SaveAs(newFile.ToString(), table, printHeader: true, sheetName: SheetName);

            watch.Stop();
            return (int)watch.Elapsed.TotalSeconds;
        }

        public static int SaveMiniExcel(string FileName, IDataReader reader, string SheetName)
        {
            if (FileName is null) throw new ArgumentNullException(nameof(FileName));
            if (reader is null) throw new ArgumentNullException(nameof(reader));

            var watch = Stopwatch.StartNew();

            var newFile = new FileInfo(FileName);
            if (newFile.Exists)
            {
                newFile.Delete();
                newFile = new FileInfo(FileName);
            }

            using (reader)
            {
                MiniExcel.SaveAs(newFile.ToString(), reader, printHeader: true, sheetName: SheetName);
            }

            watch.Stop();
            return (int)watch.Elapsed.TotalSeconds;
        }

        private static FileStream OpenReadStream(string filePath)
        {
            return new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.SequentialScan);
        }

        private static IExcelDataReader CreateExcelReader(string filePath, string sheetName, out FileStream stream)
        {
            stream = OpenReadStream(filePath);
            var reader = ExcelReaderFactory.CreateReader(stream);
            if (string.IsNullOrWhiteSpace(sheetName) || reader.Name == sheetName)
            {
                return reader;
            }

            while (reader.NextResult())
            {
                if (reader.Name == sheetName)
                {
                    return reader;
                }
            }

            reader.Dispose();
            stream.Dispose();

            stream = OpenReadStream(filePath);
            return ExcelReaderFactory.CreateReader(stream);
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
