using DBUtil;
using ExcelDataReader;
using OfficeOpenXml;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Diagnostics;
using MiniExcelLibs;
using System.Linq;

namespace DataPieCore
{
    public class ExcelIO
    {
        public static void ExcelDataReaderImport(string filePath, string tableName, IDbAccess dbAccess)
        {
                    if (filePath is null) throw new ArgumentNullException(nameof(filePath));
                    if (dbAccess is null) throw new ArgumentNullException(nameof(dbAccess));

                    // Use FileStream with SequentialScan and allow other readers
                    using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.SequentialScan);
                    using var reader = ExcelReaderFactory.CreateReader(stream);

                    // Use dataset only when multiple sheets exist or specific sheet requested.
                    var result = reader.AsDataSet(new ExcelDataSetConfiguration
                    {
                        ConfigureDataTable = _ => new ExcelDataTableConfiguration { UseHeaderRow = true }
                    });

                    DataTable dt;
                    if (result.Tables.Count == 1)
                    {
                        dt = result.Tables[0];
                    }
                    else
                    {
                        dt = result.Tables.Contains(tableName) ? result.Tables[tableName] : result.Tables[0];
                    }

                    dbAccess.BulkInsert(tableName, dt);
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

            using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.SequentialScan);
            using var reader = ExcelReaderFactory.CreateCsvReader(stream, new ExcelReaderConfiguration
            {
                FallbackEncoding = Encoding.GetEncoding("GB2312"),
                AutodetectSeparators = new[] { ',', ';', '\t', '|', '#' },
            });

            var result = reader.AsDataSet(new ExcelDataSetConfiguration
            {
                ConfigureDataTable = _ => new ExcelDataTableConfiguration { UseHeaderRow = true }
            });

            dbAccess.BulkInsert(tableName, result.Tables[0]);
        }

        public static DataTable GetDataTable(string filePath)
        {
            if (filePath is null) throw new ArgumentNullException(nameof(filePath));

            using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.SequentialScan);
            using var reader = ExcelReaderFactory.CreateReader(stream);

            var result = reader.AsDataSet(new ExcelDataSetConfiguration
            {
                ConfigureDataTable = _ => new ExcelDataTableConfiguration { UseHeaderRow = true }
            });

            return result.Tables[0];
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

            // For now we export first table with MiniExcel (MiniExcel supports multiple sheet saving via IDictionary<string, object>)
            string sql = BuildSQl.GetSQLfromTable(tableNames[0], dbtype);
            using (var reader = dbAccess.GetDataReader(sql))
            {
                MiniExcel.SaveAs(newFile.ToString(), reader);
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
    }
}