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
            var stream = File.Open(filePath, FileMode.Open, FileAccess.Read);
            var reader = ExcelDataReader.ExcelReaderFactory.CreateReader(stream);
            try
            {
                var result = reader.AsDataSet(new ExcelDataSetConfiguration()
                {
                    ConfigureDataTable = (_) => new ExcelDataTableConfiguration()
                    {
                        UseHeaderRow = true
                    }
                });

                int count = result.Tables.Count;

                if (count == 1)
                {
                    dbAccess.BulkInsert(tableName, result.Tables[0]);
                }
                else
                {
                    dbAccess.BulkInsert(tableName, result.Tables[tableName]);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                reader.Close();
                stream.Close();
            }
        }

        public static void MiniExcelReaderImport(string filePath, string tableName, IDbAccess dbAccess)
        {
            var cnt = MiniExcel.GetSheetNames(filePath).Count;

            IDataReader reader;

            if (cnt > 1)
            {
                reader = MiniExcel.GetReader(filePath, true, sheetName: tableName);
            }
            else 
            {
                reader = MiniExcel.GetReader(filePath, true);
            }

            try
            {
                dbAccess.BulkInsert(tableName, reader);
            }
            catch (Exception)
            {
                throw ;
            }
            finally
            {
                reader.Dispose();
            }

        }

        public static void CsvImport(string filePath, string tableName, IDbAccess dbAccess)
        {
            var stream = File.Open(filePath, FileMode.Open, FileAccess.Read);
            var reader = ExcelDataReader.ExcelReaderFactory.CreateCsvReader(stream, new ExcelReaderConfiguration() {

                FallbackEncoding = Encoding.GetEncoding("GB2312"),

                AutodetectSeparators = new char[] { ',', ';', '\t', '|', '#' },

            });
            try
            {
                var result = reader.AsDataSet(new ExcelDataSetConfiguration()
                {
                    ConfigureDataTable = (_) => new ExcelDataTableConfiguration()
                    {
                        UseHeaderRow = true
                    }
                });

                dbAccess.BulkInsert(tableName, result.Tables[0]);

            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                reader.Close();
                stream.Close();
            }
        }

        public static DataTable GetDataTable(string filePath)
        {
            var stream = File.Open(filePath, FileMode.Open, FileAccess.Read);
            var reader = ExcelDataReader.ExcelReaderFactory.CreateReader(stream);
            try
            {
                var result = reader.AsDataSet(new ExcelDataSetConfiguration()
                {
                    ConfigureDataTable = (_) => new ExcelDataTableConfiguration()
                    {
                        UseHeaderRow = true
                    }
                });

                return result.Tables[0];
            }
            catch (Exception ex)
            {
                throw ex;
            }

            finally
            {
                reader.Close();
                stream.Close();
            }

        }

        public static void DataTableImport(System.Data.DataTable dt, string tableName, IDbAccess dbAccess)
        {
            try
            {
                dbAccess.BulkInsert(tableName, dt);
            }

            catch (Exception ex)
            {
                throw ex;
            }

        }
  
        public static int SaveExcel(string FileName, IDataReader reader, string SheetName)
        {
            Stopwatch watch = Stopwatch.StartNew();
            watch.Start();

            ExcelPackage.LicenseContext = LicenseContext.NonCommercial;

            FileInfo newFile = new FileInfo(FileName);
            if (newFile.Exists)
            {
                newFile.Delete();
                newFile = new FileInfo(FileName);
            }
            using ExcelPackage package = new ExcelPackage(newFile);
            try
            {
                ExcelWorksheet ws = package.Workbook.Worksheets.Add(SheetName);
                ws.Cells["A1"].LoadFromDataReader(reader, true);
            }
            catch (Exception ex)
            {
                throw ex;
            }

            finally
            {
                reader.Close();
            }

            package.Save();

            watch.Stop();
            return Convert.ToInt32(watch.ElapsedMilliseconds / 1000);
        }

        public static int SaveMutiExcel(IList<string> tableNames, string filename, IDbAccess dbAccess,string dbtype)
        {
            if (filename != null)
            {               
                    Stopwatch watch = Stopwatch.StartNew();
                    watch.Start();

                    ExcelPackage.LicenseContext = LicenseContext.NonCommercial;

                    FileInfo newFile = new FileInfo(filename);
                    if (newFile.Exists)
                    {
                        newFile.Delete();
                        newFile = new FileInfo(filename);
                    }

                    using (ExcelPackage package = new ExcelPackage(newFile))
                    {
                                                            
                    foreach (var table in tableNames)
                    {
                        string sql = BuildSQl.GetSQLfromTable(table, dbtype);

                        IDataReader reader = dbAccess.GetDataReader(sql);

                        try
                        {
                            ExcelWorksheet ws = package.Workbook.Worksheets.Add(table);

                            ws.Cells["A1"].LoadFromDataReader(reader, true);

                        }

                        catch (Exception ex)
                        {
                            throw ex;
                        }

                        finally
                        {
                            reader.Close();
                        }

                    }

                    package.Save();

                    }


                    watch.Stop();

                    return Convert.ToInt32(watch.ElapsedMilliseconds / 1000);
                      
            }

            return -1;

        }

        public static int SaveMutiMiniExcel(IList<string> tableNames, string filename, IDbAccess dbAccess, string dbtype)
        {
            if (filename != null)
            {
                Stopwatch watch = Stopwatch.StartNew();
                watch.Start();

                FileInfo newFile = new FileInfo(filename);
                if (newFile.Exists)
                {
                    newFile.Delete();
                    newFile = new FileInfo(filename);
                }

                //var sheets = new Dictionary<string, object> { };

                //foreach (var table in tableNames)
                //{
                //    string sql = BuildSQl.GetSQLfromTable(table, dbtype);

                //    IDataReader reader = dbAccess.GetDataReader(sql);
                //    sheets.Add(table, reader);


                //    //System.Data.DataTable dt = dbAccess.GetDataTable(sql);
                //    //sheets.Add(table, dt);

                //}

                //MiniExcel.SaveAs(newFile.ToString(), sheets);

                string sql = BuildSQl.GetSQLfromTable(tableNames[0], dbtype);
                IDataReader reader = dbAccess.GetDataReader(sql);

                MiniExcel.SaveAs(newFile.ToString(), reader);

                reader.Close();

                watch.Stop();

                return Convert.ToInt32(watch.ElapsedMilliseconds / 1000);
            }

            return -1;
        }

        public static int SaveMiniExcel(string FileName, DataTable table, string SheetName)
        {
            Stopwatch watch = Stopwatch.StartNew();
            watch.Start();


            FileInfo newFile = new FileInfo(FileName);
            if (newFile.Exists)
            {
                newFile.Delete();
                newFile = new FileInfo(FileName);
            }

            try
            {
                MiniExcel.SaveAs(newFile.ToString(), table,printHeader:true,sheetName:SheetName);

            }
            catch (Exception ex)
            {
                throw ex;
            }



            watch.Stop();
            return Convert.ToInt32(watch.ElapsedMilliseconds / 1000);
        }

        public static int SaveMiniExcel(string FileName, IDataReader reader, string SheetName)
        {
            Stopwatch watch = Stopwatch.StartNew();
            watch.Start();


            FileInfo newFile = new FileInfo(FileName);
            if (newFile.Exists)
            {
                newFile.Delete();
                newFile = new FileInfo(FileName);
            }

            try
            {
                MiniExcel.SaveAs(newFile.ToString(), reader, printHeader: true, sheetName: SheetName);

            }
            catch (Exception ex)
            {

                throw ex;
            }


            reader.Close();

            watch.Stop();
            return Convert.ToInt32(watch.ElapsedMilliseconds / 1000);
        }
       
    }
}