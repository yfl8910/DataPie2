using System.Collections.Generic;
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
            DataSet ds = GetDataSet("select tbl_name from sqlite_master where type='table'");
            List<TableStruct> list = new List<TableStruct>();
            for (int i = 0; i < ds.Tables[0].Rows.Count; i++)
            {
                TableStruct tbl = new TableStruct
                {
                    Name = ds.Tables[0].Rows[i][0].ToString(),
                    Columns = ShowColumns(ds.Tables[0].Rows[i][0].ToString())
                };
                list.Add(tbl);
            }
            return list;
        }

        public List<Column> ShowColumns(string tablename)
        {
            List<Column> list = new List<Column>();

            DataTable dt = GetSchema("Columns", new string[] { null, null, tablename, null });
            if (dt.Rows.Count > 0)
            {
                foreach (DataRow row in dt.Rows)
                {
                    Column column = new Column();
                    column.Name = row["COLUMN_NAME"].ToString();
                    column.Desc = string.Format("{0}.{1}", row["COLUMN_NAME"].ToString(), row["DATA_TYPE"].ToString());
                    column.Type = row["DATA_TYPE"].ToString();
                    column.IsNullable = bool.Parse(row["IS_NULLABLE"].ToString());
                    column.IsPrimaryKey = bool.Parse(row["PRIMARY_Key"].ToString());
                    column.IsUnique = bool.Parse(row["Unique"].ToString());
                    column.IsNullable = bool.Parse(row["Is_Nullable"].ToString());
                    column.MaxLength= int.Parse(row["CHARACTER_MAXIMUM_LENGTH"].ToString());
                    column.IsIdentity = row["AUTOINCREMENT"].ToString()== "True";

                    list.Add(column);
                }
            }
            return list;
        }

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