using DBUtil;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Diagnostics;
using MiniExcelLibs;

namespace DataPieCore
{
    public class ExcelIO
    {

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
                throw;
            }
            finally
            {
                reader.Dispose();
            }

        }

        public static void MiniExcelReaderImport(string filePath, string tableName, IDbAccess dbAccess, bool Sqlite)
        {
            if (Sqlite)
            {

                var cnt = MiniExcel.GetSheetNames(filePath).Count;

                DataTable table;

                if (cnt > 1)
                {
                    table = MiniExcel.QueryAsDataTable(filePath, true, sheetName: tableName);
                }
                else
                {
                    table = MiniExcel.QueryAsDataTable(filePath, useHeaderRow: true);
                }

                dbAccess.BulkInsert(tableName, table);

            }
            else
            {

                MiniExcelReaderImport(filePath, tableName, dbAccess);

            }


        }

        public static void MiniExcelCsvImport(string filePath, string tableName, IDbAccess dbAccess)
        {
            var table = MiniExcel.QueryAsDataTable(filePath, useHeaderRow: true);

            try
            {
                dbAccess.BulkInsert(tableName, table);
            }
            catch (Exception)
            {
                throw;
            }

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

                var sheets = new DataSet();

                foreach (var table in tableNames)
                {
                    System.Data.DataTable dt = dbAccess.GetDataTable(BuildSQl.GetSQLfromTable(table, dbtype));
                    dt.TableName = table;
                    sheets.Tables.Add(dt);
                }

                MiniExcel.SaveAs(newFile.ToString(), sheets);

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
                MiniExcel.SaveAs(newFile.ToString(), table, printHeader: true, sheetName: SheetName);

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