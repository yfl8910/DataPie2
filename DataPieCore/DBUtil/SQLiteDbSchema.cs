using System.Collections.Generic;
using System;
using System.Linq;
using System.Data;
using System.Data.SQLite;

namespace DBUtil
{
    public partial class SQLiteDbAccess : IDbAccess
    {
        public DbSchema ShowDbSchema()
        {
            DbSchema dbs = new DbSchema
            {
                Name = GetDbName(),
                ConnectionStrings = ConnectionString,
                Dbtype = "SQLITE",
                DbTables = ShowTables(),
                DbViews = ShowViews(),
                DbProcs = GetProcs(),
                DbList = GetDataBaseInfo()
            };
            return dbs;
        }

        public List<Proc> GetProcs()
        {
            return null;
        }

        public List<string> GetDataBaseInfo()
        {
            List<string> list = null;

            DataTable databases = GetSchema("Catalogs");

            if (databases != null && databases.Rows.Count > 0)
            {
                list = new List<string>();
                foreach (DataRow database in databases.Rows)
                {
                    string name = (string)database["Catalog_name"];
                    list.Add(name);
                }
            }

            return list;
        }

        public string GetDbName()
        {
            string DBName;

            using (SQLiteConnection connection = new SQLiteConnection(ConnectionString))
            {
                DBName = connection.Database.ToString();
            }
            return DBName;
        }

        /// <summary>
        /// 获得所有表,注意返回的集合中的表模型中只有表名
        /// </summary>
        /// <returns></returns>
        public List<TableStruct> ShowTables()
        {
            using var tables = GetDataTable("select tbl_name from sqlite_master where type='table'");
            using var columns = GetSchema("Columns");
            var byTable = columns.AsEnumerable().ToLookup(r => r["TABLE_NAME"].ToString(), StringComparer.OrdinalIgnoreCase);
            return tables.AsEnumerable().Select(row => new TableStruct
            {
                Name = row[0].ToString(),
                Columns = byTable[row[0].ToString()].Select(ReadColumn).ToList()
            }).ToList();
        }

        public List<Column> ShowColumns(string tablename)
        {
            using var columns = GetSchema("Columns", new[] { null, null, tablename, null });
            return columns.AsEnumerable().Select(ReadColumn).ToList();
        }

        private static Column ReadColumn(DataRow row) => new Column
        {
            Name = row["COLUMN_NAME"].ToString(),
            Desc = $"{row["COLUMN_NAME"]}.{row["DATA_TYPE"]}",
            Type = row["DATA_TYPE"].ToString(),
            IsNullable = Convert.ToBoolean(row["IS_NULLABLE"]),
            IsPrimaryKey = Convert.ToBoolean(row["PRIMARY_KEY"]),
            IsUnique = Convert.ToBoolean(row["UNIQUE"]),
            MaxLength = row.IsNull("CHARACTER_MAXIMUM_LENGTH") ? 0 : Convert.ToInt32(row["CHARACTER_MAXIMUM_LENGTH"]),
            IsIdentity = Convert.ToBoolean(row["AUTOINCREMENT"]),
        };
        public DataTable GetSchema(string collectionName, string[] restictionValues)
        {
            using (SQLiteConnection connection = new SQLiteConnection(ConnectionString))
            {
                DataTable dt = new DataTable();
                try
                {
                    dt.Clear();
                    connection.Open();
                    dt = connection.GetSchema(collectionName, restictionValues);
                }
                catch
                {
                    dt = null;
                }

                return dt;
            }
        }

        public DataTable GetSchema(string collectionName)
        {
            using (SQLiteConnection connection = new SQLiteConnection(ConnectionString))
            {
                DataTable dt = new DataTable();
                try
                {
                    dt.Clear();
                    connection.Open();
                    dt = connection.GetSchema(collectionName);
                }
                catch
                {
                    dt = null;
                }
                return dt;
            }
        }

        public List<string> ShowViews()
        {
            List<string> List = new List<string>();
            string[] rs = new string[] { null, null, null, "BASE TABLE" };
            DataTable dt = GetSchema("Views");
            int num = dt.Rows.Count;
            if (dt.Rows.Count > 0)
            {
                foreach (DataRow _DataRowItem in dt.Rows)
                {
                    List.Add(_DataRowItem["table_name"].ToString());
                }
            }
            return List;
        }
    }
}