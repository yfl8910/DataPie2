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
            DataSet ds = GetDataSet("select TABLE_NAME,TABLE_SCHEMA from INFORMATION_SCHEMA.TABLES t where t.TABLE_TYPE ='BASE TABLE'");
            List<TableStruct> list = new List<TableStruct>();

            var allcolumns = AllColumns();

            var allForeignKeys = AllForeignKeys();

            for (int i = 0; i < ds.Tables[0].Rows.Count; i++)
            {
                TableStruct tbl = new TableStruct();
                tbl.Name = ds.Tables[0].Rows[i][0].ToString();
                tbl.TableSchemaName = ds.Tables[0].Rows[i][1].ToString();
                tbl.Columns = ShowColumns(allcolumns,ds.Tables[0].Rows[i][0].ToString());
                tbl.ForeignKeys = ShowForeignKeys1(allForeignKeys, ds.Tables[0].Rows[i][0].ToString());
                tbl.PrimaryKey = tbl.Columns.Where(p => p.IsPrimaryKey == true).Select(p => p.Name).FirstOrDefault();

                //tbl.Constraints = ShowConstraints(ds.Tables[0].Rows[i][0].ToString());
                list.Add(tbl);
            }
            return list;
        }
        public List<Column> ShowColumns(DataTable dt,string tablename)
        {
            List<Column> list = new List<Column>();

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                if (dt.Rows[i]["TableName"].ToString()==tablename) {

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

   
            }

            return list;
        }

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
	OBJECT_SCHEMA_NAME(f.parent_object_id) AS TableSchemaName,
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
            List<ForeignKeySchema> list = new List<ForeignKeySchema>();


            for (int i = 0; i < dt.Rows.Count; i++)
            {
                if (dt.Rows[i]["TableName"].ToString() == tablename)
                {

                    ForeignKeySchema fkc = new ForeignKeySchema();

                    fkc.ColumnName = dt.Rows[i]["ColumnName"].ToString();
                    fkc.ForeignTableName = dt.Rows[i]["ReferenceTableName"].ToString();
                    fkc.ForeignColumnName = dt.Rows[i]["ReferenceColumnName"].ToString();
                    fkc.TableName = tablename;
                    fkc.CascadeOnDelete = dt.Rows[i]["delete_referential_action_desc"].ToString() == "CASCADE";

                    list.Add(fkc);
                }


            }

            return list;
        }


        public List<ForeignKeySchema> ShowForeignKeys2(string tablename)
        {
            List<ForeignKeySchema> list = new List<ForeignKeySchema>();

          string sql=  string.Format(
                       @"SELECT 
	OBJECT_SCHEMA_NAME(f.parent_object_id) AS TableSchemaName,
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
            var List = new List<string>();
            //string[] rs = new string[] { null, null, null, "BASE TABLE" };
            DataTable dt = GetSchema("views");

            if (dt.Rows.Count > 0)
            {
                foreach (DataRow _DataRowItem in dt.Rows)
                {
                    List.Add(_DataRowItem["table_name"].ToString());
                }
            }
            return List;
        }

        public List<ViewSchema> ShowViews2() 
        {
            var List = new List<ViewSchema>();

            string sql = @"SELECT TABLE_NAME, VIEW_DEFINITION  from INFORMATION_SCHEMA.VIEWS";

            SqlCommand cmd = new SqlCommand(sql, (SqlConnection)conn);

            conn.Open();

            using (SqlDataReader reader = cmd.ExecuteReader(CommandBehavior.CloseConnection))
            {
                while (reader.Read())
                {
                    ViewSchema view = new ViewSchema();
                    view.ViewName = (string)reader["TABLE_NAME"];
                    view.ViewSQL = (string)reader["VIEW_DEFINITION"];
                    List.Add(view);

                }
            }
            return List;


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
        public List<Proc> GetProcs()
        {
            List<Proc> res = new List<Proc>();
            string sql = @"select 名称=ROUTINE_NAME,最近更新=LAST_ALTERED
from INFORMATION_SCHEMA.ROUTINES
where ROUTINE_TYPE='PROCEDURE'";
            DataTable dt = GetDataTable(sql);
            if (dt.Rows.Count > 0)
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    Proc proc = new Proc
                    {
                        Name = dt.Rows[i]["名称"].ToString(),
                        LastUpdate = dt.Rows[i]["最近更新"].ToString()
                    };

                    //去除参数表和存储过程定义，提升速度

                    //sql = @"sp_helptext '" + proc.Name + "'";
                    //StringBuilder sb = new StringBuilder("");
                    //DataTable dt2 = GetDataTable(sql);
                    //if (dt2.Rows.Count > 0)
                    //{
                    //    for (int ii = 0; ii < dt2.Rows.Count; ii++)
                    //    {
                    //        sb.AppendLine(dt2.Rows[ii][0].ToString());
                    //    }
                    //    proc.CreateSql = sb.ToString();
                    //}

                    //string sql2 = @"select '参数名称' = name,
                    //'类型' = type_name(xusertype),
                    //'长度' = length
                    //from syscolumns
                    //where id = object_id('" + proc.Name + "')";
                    //DataTable dt3 = GetDataTable(sql2);
                    //List<Procparam> p = new List<Procparam>();
                    //if (dt3.Rows.Count > 0)
                    //{
                    //    for (int ii = 0; ii < dt3.Rows.Count; ii++)
                    //    {
                    //        Procparam param1 = new Procparam
                    //        {
                    //            Name = dt3.Rows[ii]["参数名称"].ToString(),
                    //            Type = dt3.Rows[ii]["类型"].ToString(),
                    //            Length = dt3.Rows[ii]["长度"].ToString()
                    //        };
                    //        p.Add(param1);
                    //    }
                    //}
                    //proc.Param = p;

                    res.Add(proc);
                }
            }
            return res;
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