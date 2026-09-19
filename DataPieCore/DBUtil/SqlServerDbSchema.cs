using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace DBUtil
{
    public partial class SqlServerDbAccess : IDbAccess
    {
        public DbSchema ShowDbSchema()
        {
            DbSchema dbs = new DbSchema
            {
                Name = GetDbName(),
                ConnectionStrings = ConnectionString,
                Dbtype = "SQLSERVER",
                DbTables = ShowTables(),
                DbViews = ShowViews(),
                //DbViews2 = ShowViews2(),
                DbProcs = GetProcs()
                   //DbList = GetDataBaseInfo()
               };
            return dbs;
        }

        /// <summary>
        /// 获得所有表
        /// </summary>
        /// <returns></returns>

        public List<TableStruct> ShowTables()
        {
            using var allColumns = AllColumns();
            using var allForeignKeys = AllForeignKeys();
            var columnsByTable = allColumns.AsEnumerable().ToLookup(r => Convert.ToInt32(r["TableId"]));
            var keysByTable = allForeignKeys.AsEnumerable().ToLookup(r => Convert.ToInt32(r["TableId"]));
            const string sql = "SELECT name, SCHEMA_NAME(schema_id), object_id FROM sys.tables";
            using var reader = GetDataReader(sql);
            var tables = new List<TableStruct>();
            while (reader.Read())
            {
                int tableId = reader.GetInt32(2);
                var columns = columnsByTable[tableId].Select(ReadColumn).ToList();
                tables.Add(new TableStruct
                {
                    Name = reader.GetString(0),
                    TableSchemaName = reader.GetValue(1).ToString(),
                    Columns = columns,
                    ForeignKeys = keysByTable[tableId].Select(ReadForeignKey).ToList(),
                    PrimaryKey = columns.FirstOrDefault(column => column.IsPrimaryKey)?.Name
                });
            }
            return tables;
        }
        public List<Column> ShowColumns(DataTable dt, string tablename)
        {
            return dt.AsEnumerable().Where(r => r["TableName"].ToString() == tablename).Select(ReadColumn).ToList();
        }

        private static Column ReadColumn(DataRow row) => new Column
        {
            Name = row["DbColumnName"].ToString(),
            Desc = row["ColumnDescription"].ToString(),
            IsIdentity = Convert.ToInt32(row["IsIdentity"]) == 1,
            IsNullable = Convert.ToInt32(row["IsNullable"]) == 1,
            Type = row["DataType"].ToString(),
            Default = row["DefaultValue"].ToString(),
            MaxLength = Convert.ToInt32(row["Length"]),
            IsPrimaryKey = Convert.ToInt32(row["IsPrimaryKey"]) == 1,
        };
        public DataTable AllColumns()
        {

            string sql = string.Format(@"SELECT sysobjects.name AS TableName,
                           syscolumns.Id AS TableId,
                           syscolumns.name AS DbColumnName,
                           systypes.name AS DataType,
                           syscolumns.length AS [Length],
                           sys.extended_properties.[value] AS [ColumnDescription],
                           syscomments.text AS DefaultValue,
                           syscolumns.isnullable AS IsNullable,
	                       columnproperty(syscolumns.id,syscolumns.name,'IsIdentity')as IsIdentity,
                           (CASE
                                WHEN EXISTS
                                       (
                                             	select 1
												from sysindexes i
												join sysindexkeys k on i.id = k.id and i.indid = k.indid
												join sysobjects o on i.id = o.id
												join syscolumns c on i.id=c.id and k.colid = c.colid
												where o.xtype = 'U'
												and exists(select 1 from sysobjects where xtype = 'PK' and name = i.name)
												and o.name=sysobjects.name and c.name=syscolumns.name
                                       ) THEN 1
                                ELSE 0
                            END) AS IsPrimaryKey
                    FROM syscolumns
                    INNER JOIN systypes ON syscolumns.xtype = systypes.xtype
                    LEFT JOIN sysobjects ON syscolumns.id = sysobjects.id
                    LEFT OUTER JOIN sys.extended_properties ON (sys.extended_properties.minor_id = syscolumns.colid
                                                                AND sys.extended_properties.major_id = syscolumns.id)
                    LEFT OUTER JOIN syscomments ON syscolumns.cdefault = syscomments.id
                    WHERE syscolumns.id IN
                        (SELECT id
                         FROM sysobjects
                         WHERE xtype IN('u',
                                        'v') )
                      AND (systypes.name <> 'sysname')
                      AND systypes.name<>'geometry'
                      AND systypes.name<>'geography'
                    ORDER BY syscolumns.colid");
            DataTable dt = GetDataTable(sql);

            return dt;

        }

    
        public DataTable AllForeignKeys()
        {


            string sql = string.Format(
                         @"SELECT 
	f.parent_object_id AS TableId, OBJECT_SCHEMA_NAME(f.parent_object_id) AS TableSchemaName,
	OBJECT_NAME(f.parent_object_id) AS TableName,
	COL_NAME(fc.parent_object_id, fc.parent_column_id) AS ColumnName,
	f.name AS ForeignKeyName,
	OBJECT_SCHEMA_NAME(f.referenced_object_id) AS ReferenceTableSchemaName,
	OBJECT_NAME(f.referenced_object_id) AS ReferenceTableName,
	COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS ReferenceColumnName,
	f.update_referential_action_desc,
	f.delete_referential_action_desc
FROM 
	sys.foreign_keys AS f INNER JOIN sys.foreign_key_columns AS fc
		ON f.OBJECT_ID = fc.constraint_object_id");
            DataTable dt = GetDataTable(sql);

            return dt;

        }

        public List<ForeignKeySchema> ShowForeignKeys1(DataTable dt, string tablename)
        {
            return dt.AsEnumerable().Where(r => r["TableName"].ToString() == tablename).Select(ReadForeignKey).ToList();
        }

        private static ForeignKeySchema ReadForeignKey(DataRow row) => new ForeignKeySchema
        {
            ColumnName = row["ColumnName"].ToString(),
            ForeignTableName = row["ReferenceTableName"].ToString(),
            ForeignColumnName = row["ReferenceColumnName"].ToString(),
            TableName = row["TableName"].ToString(),
            CascadeOnDelete = row["delete_referential_action_desc"].ToString() == "CASCADE",
        };
        public List<ForeignKeySchema> ShowForeignKeys2(string tablename)
        {
            List<ForeignKeySchema> list = new List<ForeignKeySchema>();

          string sql=  string.Format(
                       @"SELECT 
	f.parent_object_id AS TableId, OBJECT_SCHEMA_NAME(f.parent_object_id) AS TableSchemaName,
	OBJECT_NAME(f.parent_object_id) AS TableName,
	COL_NAME(fc.parent_object_id, fc.parent_column_id) AS ColumnName,
	f.name AS ForeignKeyName,
	OBJECT_SCHEMA_NAME(f.referenced_object_id) AS ReferenceTableSchemaName,
	OBJECT_NAME(f.referenced_object_id) AS ReferenceTableName,
	COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS ReferenceColumnName,
	f.update_referential_action_desc,
	f.delete_referential_action_desc
FROM 
	sys.foreign_keys AS f INNER JOIN sys.foreign_key_columns AS fc
		ON f.OBJECT_ID = fc.constraint_object_id
WHERE OBJECT_NAME(f.parent_object_id) = '{0}'
",
                       tablename);

            SqlCommand cmd = new SqlCommand(sql, (SqlConnection)conn);

            conn.Open();

            using (SqlDataReader reader = cmd.ExecuteReader(CommandBehavior.CloseConnection))
            {
                while (reader.Read())
                {
                    ForeignKeySchema fkc = new ForeignKeySchema();
                    fkc.ColumnName = (string)reader["ColumnName"];
                    fkc.ForeignTableName = (string)reader["ReferenceTableName"];
                    fkc.ForeignColumnName = (string)reader["ReferenceColumnName"];
                    fkc.CascadeOnDelete = (string)reader["delete_referential_action_desc"] == "CASCADE";
                    //fkc.IsNullable = (string)reader["IsNullable"] == "YES";
                    fkc.TableName = tablename;
                    list.Add(fkc);
                }
            }
            return list;
        }

        public List<Constraint> ShowConstraints(string tablename)
        {
            List<Constraint> cons = new List<Constraint>();

            string sql = "sp_helpconstraint @objname='" + tablename + "'";
            DataSet ds = GetDataSet(sql);
            if (ds.Tables.Count > 1 && ds.Tables[1].Rows.Count > 0)
            {
                for (int i = 0; i < ds.Tables[1].Rows.Count; i++)
                {
                    Constraint constraint = new Constraint();

                    string type = (ds.Tables[1].Rows[i]["constraint_type"] ?? "").ToString();
                    if (string.IsNullOrWhiteSpace(type))
                    {
                        continue;
                    }
                    if (type.StartsWith("CHECK"))
                    {
                        constraint.Name = (ds.Tables[1].Rows[i]["constraint_name"] ?? "").ToString();
                        constraint.Type = "CHECK";
                        string[] arr = (ds.Tables[1].Rows[i]["constraint_type"] ?? "").ToString().Split(new String[] { " " }, StringSplitOptions.RemoveEmptyEntries);

                        constraint.Keys = arr[arr.Length - 1];
                        constraint.Remark = (ds.Tables[1].Rows[i]["constraint_keys"] ?? "").ToString();
                        cons.Add(constraint);
                        continue;
                    }
                    else if (type.StartsWith("DEFAULT"))
                    {
                        constraint.Name = (ds.Tables[1].Rows[i]["constraint_name"] ?? "").ToString();
                        constraint.Type = "DEFAULT";
                        string[] arr = (ds.Tables[1].Rows[i]["constraint_type"] ?? "").ToString().Split(new String[] { " " }, StringSplitOptions.RemoveEmptyEntries);

                        constraint.Keys = arr[arr.Length - 1];
                        constraint.Remark = (ds.Tables[1].Rows[i]["constraint_keys"] ?? "").ToString();
                        cons.Add(constraint);
                        continue;
                    }
                    else if (type.StartsWith("FOREIGN"))
                    {
                        constraint.Name = (ds.Tables[1].Rows[i]["constraint_name"] ?? "").ToString();
                        constraint.Type = "FOREIGN";
                        if (ds.Tables[1].Rows.Count > i)
                        {
                            constraint.Name = (ds.Tables[1].Rows[i]["constraint_name"] ?? "").ToString();
                            constraint.Keys = (ds.Tables[1].Rows[i]["constraint_keys"] ?? "").ToString();
                            constraint.DelType = (ds.Tables[1].Rows[i]["delete_action"] ?? "").ToString();
                            constraint.UpdateType = (ds.Tables[1].Rows[i]["update_action"] ?? "").ToString();

                            DataTable dt2 = GetDataTable("select delete_referential_action,update_referential_action from sys.foreign_keys where name='" + constraint.Name + "'");
                            switch (dt2.Rows[0]["delete_referential_action"].ToString())
                            {
                                case "0":
                                    {
                                        constraint.DelType = "NO ACTION";
                                        break;
                                    }
                                case "1":
                                    {
                                        constraint.DelType = "CASCADE";
                                        break;
                                    }
                                case "2":
                                    {
                                        constraint.DelType = "SET NULL";
                                        break;
                                    }
                                case "3":
                                    {
                                        constraint.DelType = "SET DEFAULT";
                                        break;
                                    }
                            }
                            switch (dt2.Rows[0]["update_referential_action"].ToString())
                            {
                                case "0":
                                    {
                                        constraint.UpdateType = "NO ACTION";
                                        break;
                                    }
                                case "1":
                                    {
                                        constraint.UpdateType = "CASCADE";
                                        break;
                                    }
                                case "2":
                                    {
                                        constraint.UpdateType = "SET NULL";
                                        break;
                                    }
                                case "3":
                                    {
                                        constraint.UpdateType = "SET DEFAULT";
                                        break;
                                    }
                            }
                            constraint.RefStr = (ds.Tables[1].Rows[i + 1]["constraint_keys"] ?? "").ToString();
                        }
                        constraint.Remark = "Update(" + constraint.UpdateType + ")," + "Delete(" + constraint.DelType + "),Ref(" + constraint.RefStr + ")";
                        cons.Add(constraint);
                        continue;
                    }
                    else if (type.StartsWith("PRIMARY"))
                    {
                        constraint.Name = (ds.Tables[1].Rows[i]["constraint_name"] ?? "").ToString();
                        constraint.Type = "PRIMARY";
                        constraint.Keys = (ds.Tables[1].Rows[i]["constraint_keys"] ?? "").ToString();
                        cons.Add(constraint);
                    }
                    else if (type.StartsWith("UNIQUE"))
                    {
                        constraint.Name = (ds.Tables[1].Rows[i]["constraint_name"] ?? "").ToString();
                        constraint.Type = "UNIQUE";
                        constraint.Keys = (ds.Tables[1].Rows[i]["constraint_keys"] ?? "").ToString();
                        cons.Add(constraint);
                    }
                }
            }

            return cons;
        }

        public List<string> ShowViews()
        {
            const string sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS";
            using var reader = GetDataReader(sql);
            var views = new List<string>();
            while (reader.Read())
                views.Add(reader.GetString(0));
            return views;
        }

        public List<ViewSchema> ShowViews2()
        {
            const string sql = @"SELECT s.name AS SchemaName, v.name AS ViewName, m.definition
FROM sys.views v
JOIN sys.schemas s ON s.schema_id = v.schema_id
LEFT JOIN sys.sql_modules m ON m.object_id = v.object_id
WHERE v.is_ms_shipped = 0
ORDER BY s.name, v.name";
            using var reader = GetDataReader(sql);
            var views = new List<ViewSchema>();
            while (reader.Read())
                views.Add(new ViewSchema
                {
                    SchemaName = reader.GetString(0),
                    ViewName = reader.GetString(1),
                    ViewSQL = reader.IsDBNull(2) ? null : reader.GetString(2)
                });
            return views;
        }

        public string GetDbName()
        {
            string DBName;

            using (SqlConnection connection = new SqlConnection(ConnectionString))
            {
                DBName = connection.Database.ToString();
            }
            return DBName;
        }

        public DataTable GetSchema(string collectionName)
        {
            using SqlConnection connection = new SqlConnection(ConnectionString);
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

        /// <summary>
        /// 根据条件，返回架构信息	
        /// </summary>
        /// <param name="collectionName">集合名称</param>
        /// <param name="restictionValues">约束条件</param>
        /// <returns>DataTable</returns>
        public  DataTable GetSchema(string collectionName, string[] restictionValues)
        {
            using (SqlConnection connection = new SqlConnection(ConnectionString))
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

        /// <summary>
        /// 获取当前数据库的用户自定义存储过程
        /// </summary>
        /// <returns></returns>
        public List<Proc> ReadProcedureDefinitions()
        {
            const string sql = @"SELECT s.name, p.name, m.definition
FROM sys.procedures p
JOIN sys.schemas s ON s.schema_id = p.schema_id
LEFT JOIN sys.sql_modules m ON m.object_id = p.object_id
WHERE p.is_ms_shipped = 0 ORDER BY s.name, p.name";
            using var reader = GetDataReader(sql);
            var procedures = new List<Proc>();
            while (reader.Read())
                procedures.Add(new Proc
                {
                    SchemaName = reader.GetString(0),
                    Name = reader.GetString(1),
                    CreateSql = reader.IsDBNull(2) ? null : reader.GetString(2)
                });
            return procedures;
        }

        public List<Proc> GetProcs()
        {
            const string sql = @"SELECT ROUTINE_NAME, LAST_ALTERED
FROM INFORMATION_SCHEMA.ROUTINES
WHERE ROUTINE_TYPE = 'PROCEDURE'";
            using var reader = GetDataReader(sql);
            var procedures = new List<Proc>();
            while (reader.Read())
                procedures.Add(new Proc
                {
                    Name = reader.GetString(0),
                    LastUpdate = reader.GetValue(1).ToString()
                });
            return procedures;
        }

        /// <summary>
        /// 获取当前数据库列表
        /// </summary>
        /// <returns></returns>
        public List<string> GetDataBaseInfo()
        {
            string sql = "SELECT  Name FROM Master..SysDatabases where Name not in('master', 'tempdb', 'model', 'msdb', 'ReportServer', 'ReportServerTempDB')";

            DataTable dt = GetDataTable(sql);

            List<string> DatabaseList = new List<string>();

            if (dt.Rows.Count > 0)
            {
                foreach (DataRow _DataRowItem in dt.Rows)
                {
                    DatabaseList.Add(_DataRowItem["Name"].ToString());
                }
            }
            return DatabaseList;
        }

  
    }
}
