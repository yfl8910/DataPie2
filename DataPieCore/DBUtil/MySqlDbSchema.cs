using MySql.Data.MySqlClient;
using System.Collections.Generic;
using System.Data;

namespace DBUtil
{
    public partial class MySqlDbAccess : IDbAccess
    {
        public DbSchema ShowDbSchema()
        {
            IsKeepConnect = true;
            DbSchema dbs = new DbSchema
            {
                Name = GetDbName(),
                ConnectionStrings = ConnectionString,
                Dbtype = "MYSQL",
                DbTables = ShowTables(),
                DbViews = ShowViews(),
                DbProcs = GetProcs(),
                DbList = GetDataBaseInfo()
            };

            IsKeepConnect = false;
            return dbs;
        }

        public List<string> GetDataBaseInfo()
        {
            List<string> list = null;

            DataTable databases = GetSchema("Databases");

            if (databases != null && databases.Rows.Count > 0)
            {
                list = new List<string>();
                foreach (DataRow database in databases.Rows)
                {
                    string name = (string)database["database_name"];
                    list.Add(name);
                }
            }

            return list;
        }

        public List<TableStruct> ShowTables()
        {
            List<TableStruct> list = new List<TableStruct>();

            DataTable dt = GetSchema("tables");
            int num = dt.Rows.Count;
            if (dt.Rows.Count > 0)
            {
                foreach (DataRow _DataRowItem in dt.Rows)
                {
                    TableStruct tbl = new TableStruct();
                    tbl.Name = _DataRowItem["TABLE_NAME"].ToString();
                    tbl.Desc = string.Format("{0}.{1}", _DataRowItem["TABLE_SCHEMA"].ToString(), _DataRowItem["TABLE_NAME"].ToString());
                    tbl.Columns = ShowColumns(_DataRowItem["TABLE_NAME"].ToString());
                    list.Add(tbl);
                }
            }
            return list;
        }

        public List<Column> ShowColumns(string tablename)
        {
            List<Column> list = new List<Column>();

            string sql = string.Format(@"SELECT TABLE_NAME as TableName,
                                    column_name AS DbColumnName,
                                    CASE WHEN  left(COLUMN_TYPE,LOCATE('(',COLUMN_TYPE)-1)='' THEN COLUMN_TYPE ELSE  left(COLUMN_TYPE,LOCATE('(',COLUMN_TYPE)-1) END   AS DataType,
                                    CAST(SUBSTRING(COLUMN_TYPE,LOCATE('(',COLUMN_TYPE)+1,LOCATE(')',COLUMN_TYPE)-LOCATE('(',COLUMN_TYPE)-1) AS signed) AS Length,
                                    column_default  AS  `DefaultValue`,
                                    column_comment  AS  `ColumnDescription`,
                                    CASE WHEN COLUMN_KEY = 'PRI'
                                    THEN true ELSE false END AS `IsPrimaryKey`,
                                    CASE WHEN EXTRA='auto_increment' THEN true ELSE false END as IsIdentity,
                                    CASE WHEN is_nullable = 'YES'
                                    THEN true ELSE false END AS `IsNullable`
                                    FROM
                                    Information_schema.columns where TABLE_NAME='{0}' and  TABLE_SCHEMA=(select database()) ORDER BY TABLE_NAME", tablename);
            DataTable dt = GetDataTable(sql);
            if (dt.Rows.Count == 0)
            {
                return null;
            }

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                Column col = new Column()
                {
                    Name = dt.Rows[i]["DbColumnName"].ToString(),
                    Desc = dt.Rows[i]["ColumnDescription"].ToString(),
                    IsIdentity = int.Parse(dt.Rows[i]["IsIdentity"].ToString()) == 1,
                    IsNullable = int.Parse(dt.Rows[i]["IsNullable"].ToString()) == 1,
                    Type = dt.Rows[i]["DataType"].ToString(),
                    Default = dt.Rows[i]["DefaultValue"].ToString(),
                    MaxLength = int.Parse(dt.Rows[i]["Length"].ToString()),
                    IsPrimaryKey = int.Parse(dt.Rows[i]["IsPrimaryKey"].ToString()) == 1,
                };

                list.Add(col);
            }

            return list;
        }

        public string GetDbName()
        {
            string DBName;

            using (MySqlConnection connection = new MySqlConnection(ConnectionString))
            {
                DBName = connection.Database.ToString();
            }
            return DBName;
        }

        public List<string> ShowViews()
        {
            var List = new List<string>();
            DataTable dt = GetSchema("views");
            string DBName = GetDbName();

            if (dt.Rows.Count > 0)
            {
                foreach (DataRow _DataRowItem in dt.Rows)
                {
                    if (_DataRowItem["TABLE_SCHEMA"].ToString() == DBName)
                    {
                        List.Add(_DataRowItem["TABLE_NAME"].ToString());
                    }
                }
            }
            return List;
        }

        public DataTable GetSchema(string collectionName)
        {
            using (MySqlConnection connection = new MySqlConnection(ConnectionString))
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

        public DataTable GetSchema(string collectionName, string[] restictionValues)
        {
            using (MySqlConnection connection = new MySqlConnection(ConnectionString))
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

        public List<Proc> GetProcs()
        {
            List<Proc> list = new List<Proc>();

            DataTable dt = GetSchema("Procedures");

            string DBName = GetDbName(); ;

            if (dt.Rows.Count > 0)
            {
                foreach (DataRow _DataRowItem in dt.Rows)
                {
                    if (_DataRowItem["ROUTINE_SCHEMA"].ToString() == DBName)
                    {

                        Proc proc = new Proc();
                        proc.Name = _DataRowItem["SPECIFIC_NAME"].ToString();
                        proc.CreateSql = _DataRowItem["ROUTINE_DEFINITION"].ToString();
                        string sql2 = string.Format(@"SELECT SPECIFIC_SCHEMA,SPECIFIC_NAME,PARAMETER_NAME,ORDINAL_POSITION,PARAMETER_MODE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH
                                                      FROM INFORMATION_SCHEMA.PARAMETERS where SPECIFIC_NAME='{0}'", proc.Name);
                        DataTable dt3 = GetDataTable(sql2);
                        List<Procparam> p = new List<Procparam>();
                        if (dt3.Rows.Count > 0)
                        {
                            for (int ii = 0; ii < dt3.Rows.Count; ii++)
                            {
                                Procparam param1 = new Procparam
                                {
                                    Name = dt3.Rows[ii]["PARAMETER_NAME"].ToString(),
                                    Type = dt3.Rows[ii]["DATA_TYPE"].ToString(),
                                    Length = dt3.Rows[ii]["CHARACTER_MAXIMUM_LENGTH"].ToString()
                                };
                                p.Add(param1);
                            }
                        }
                        proc.Param = p;

                        list.Add(proc);
                    }
                }
            }
            return list;
        }
    }
}