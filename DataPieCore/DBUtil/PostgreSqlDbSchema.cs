using Npgsql;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace DBUtil
{
    public partial class PostgreSqlDbAccess : IDbAccess
    {
        
        public DbSchema ShowDbSchema()
        {
            IsKeepConnect = true;
            DbSchema dbs = new DbSchema
            {
                ConnectionStrings = ConnectionString,
                Dbtype = "POSTGRESQL",
                DbTables = ShowTables(),
                DbViews = ShowViews(),
                DbProcs = GetProcs()
            };

            IsKeepConnect = false;
            return dbs;
        }

        public List<TableStruct> ShowTables()
        {
            DataSet ds = GetDataSet("select TABLE_NAME from INFORMATION_SCHEMA.TABLES t where t.TABLE_TYPE ='BASE TABLE' and table_schema='public'");
            List<TableStruct> list = new List<TableStruct>();
            for (int i = 0; i < ds.Tables[0].Rows.Count; i++)
            {
                TableStruct tbl = new TableStruct();
                tbl = GetTableStruct(ds.Tables[0].Rows[i][0].ToString());
                list.Add(tbl);
                //TableStruct tbl = new TableStruct
                //{
                //    Name = ds.Tables[0].Rows[i][0].ToString()
                // };

                //tbl.Columns = ShowColumns(ds.Tables[0].Rows[i][0].ToString());
                //list.Add(tbl);
            }
            return list;
        }

        public List<Column> ShowColumns(string tablename)
        {
            List<Column> list = new List<Column>();

            string sql = string.Format(@"select cast (pclass.oid as int4) as TableId,cast(ptables.tablename as varchar) as TableName,
                                pcolumn.column_name as DbColumnName,pcolumn.udt_name as DataType,
                                pcolumn.character_maximum_length as Length,
                                pcolumn.column_default as DefaultValue,
                                col_description(pclass.oid, pcolumn.ordinal_position) as ColumnDescription,
                                case when pkey.colname = pcolumn.column_name
                                then true else false end as IsPrimaryKey,
                                case when pcolumn.column_default like 'nextval%'
                                then true else false end as IsIdentity,
                                case when pcolumn.is_nullable = 'YES'
                                then true else false end as IsNullable
                                 from (select * from pg_tables where tablename = '{0}' and schemaname='public') ptables inner join pg_class pclass
                                on ptables.tablename = pclass.relname inner join (SELECT *
                                FROM information_schema.columns
                                ) pcolumn on pcolumn.table_name = ptables.tablename
                                left join (
	                                select  pg_class.relname,pg_attribute.attname as colname from
	                                pg_constraint  inner join pg_class
	                                on pg_constraint.conrelid = pg_class.oid
	                                inner join pg_attribute on pg_attribute.attrelid = pg_class.oid
	                                and  pg_attribute.attnum = pg_constraint.conkey[1]
	                                inner join pg_type on pg_type.oid = pg_attribute.atttypid
	                                where pg_constraint.contype='p'
                                ) pkey on pcolumn.table_name = pkey.relname
                                order by ptables.tablename", tablename);
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

        public List<string> ShowViews()
        {
            var List = new List<string>();
            DataTable dt = GetSchema("views");
            if (dt.Rows.Count > 0)
            {
                foreach (DataRow _DataRowItem in dt.Rows)
                {
                    if (_DataRowItem["TABLE_SCHEMA"].ToString() != "sys" && _DataRowItem["TABLE_SCHEMA"].ToString() != "sakila")
                    {
                        List.Add(_DataRowItem["TABLE_NAME"].ToString());
                    }
                }
            }
            return List;
        }

        public DataTable GetSchema(string collectionName)
        {
            using NpgsqlConnection connection = new NpgsqlConnection(ConnectionString);
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

        public List<string> GetDataBaseInfo()
        {
            return null;
        }

        /// <summary>
        /// 获得指定表的表结构说明
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <returns></returns>
        public TableStruct GetTableStruct(string tableName)
        {
            string wrapTableName = "";
            if (!string.IsNullOrWhiteSpace(tableName) || !tableName.StartsWith("\""))
            {
                wrapTableName = "\"" + tableName + "\"";
            }
            string sql = string.Format(@"
select 序号,列名,类型,长度,变长长度,是否可空,d.description 说明,tt.存在默认值
  from (SELECT c.oid        oid,
               a.attnum     attnum,
                ROW_NUMBER() OVER(order BY a.attnum) 序号,
               a.attname    列名,
               a.atthasdef  存在默认值,
               t.typname    类型,
               a.attlen     长度,
               a.atttypmod 变长长度,
               a.attnotnull 是否可空
          from pg_class c, pg_attribute a, pg_type t
         where c.relname = '{0}'
           and a.attnum > 0
           and a.attrelid = c.oid
	   and a.attisdropped='f'
           and a.atttypid = t.oid) tt
  left join pg_description d on d.objoid = tt.oid
                            and d.objsubid = tt.attnum
  left join pg_attrdef def on def.adrelid=tt.oid and tt.attnum=def.adnum
", tableName);
            DataTable dt = GetDataTable(sql);
            if (dt.Rows.Count == 0)
            {
                return null;
            }
            TableStruct tbl = new TableStruct
            {
                Name = tableName
            };
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                Column col = new Column()
                {
                    Name = dt.Rows[i]["列名"].ToString(),
                    Desc = dt.Rows[i]["说明"].ToString(),
                    IsNullable = dt.Rows[i]["是否可空"].ToString().ToUpper() == "FALSE" ? true : false,
                    Type = dt.Rows[i]["类型"].ToString(),
                    MaxLength = int.Parse(dt.Rows[i]["变长长度"].ToString()),
                    //HasDefault = dt.Rows[i]["存在默认值"].ToString().ToUpper() == "TRUE" ? true : false,
                    //Default = (dt.Rows[i]["默认值"] ?? "").ToString()
                };
                tbl.Columns.Add(col);
            }
            //找主键字段
            string primarySql = string.Format(@"
select pg_attribute.attname  as colname
  from pg_constraint
 inner join pg_class on pg_constraint.conrelid = pg_class.oid
 inner join pg_attribute on pg_attribute.attrelid = pg_class.oid
                        and array[pg_attribute.attnum] <@ pg_constraint.conkey
 inner join pg_type on pg_type.oid = pg_attribute.atttypid
 where pg_class.relname = '{0}'
   and pg_constraint.contype = 'p'
", tableName);
            dt = GetDataTable(primarySql);
            if (dt.Rows.Count > 0)
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    if (i == 0)
                    {
                        tbl.PrimaryKey = dt.Rows[0][0].ToString();
                    }
                    else
                    {
                        tbl.PrimaryKey += "," + dt.Rows[i][0].ToString();
                    }
                }
                if (dt.Rows.Count > 1)
                {
                    for (int i = 0; i < dt.Rows.Count; i++)
                    {
                        var col = tbl.Columns.Find(j => j.Name == dt.Rows[i][0].ToString());
                        col.IsUnique = true;
                        //col.IsUniqueUion = true;
                        //col.UniqueCols = tbl.PrimaryKey; ;
                    }
                }
                else if (dt.Rows.Count == 1)
                {
                    var col = tbl.Columns.Find(j => j.Name == dt.Rows[0][0].ToString());
                    col.IsUnique = true;
                    //col.IsUniqueUion = false;
                }
            }
            //找唯一索引，注意,可能是两个列联合唯一
            string uqiSql = string.Format(@"
select t.attname 列名,cc.relname  索引名 ,t.relname 表名 from (select a.attname, x.indexrelid,c.relname
  from pg_index x, pg_class c, pg_attribute a
 where c.oid = x.indrelid
   and a.attrelid = x.indexrelid
   and indisunique = true
   and indisprimary=false
   and c.relname = '{0}') t left join pg_class cc on t.indexrelid=cc.oid
  order by 索引名", tableName);
            dt = GetDataTable(uqiSql);
            if (dt.Rows.Count > 0)
            {
                Hashtable ht = new Hashtable();
                //首先找出来哪些索引名称是联合索引
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    if (ht[dt.Rows[i]["索引名"]] == null)
                    {
                        ht.Add(dt.Rows[i]["索引名"].ToString(), new List<string>() { dt.Rows[i]["列名"].ToString() });
                    }
                    else
                    {
                        //如果不为空,说明出现了联合索引
                        ((List<string>)ht[dt.Rows[i]["索引名"].ToString()]).Add(dt.Rows[i]["列名"].ToString());
                    }
                }
                for (int i = 0; i < tbl.Columns.Count; i++)
                {
                    var keys = ht.Keys;
                    foreach (var item in keys)
                    {
                        if (((List<string>)ht[item.ToString()]).Contains(tbl.Columns[i].Name))
                        {
                            //如果这个列在某一个索引名称的索引列中
                            tbl.Columns[i].IsUnique = true;
                            string cols = "";
                            ((List<string>)ht[item.ToString()]).ForEach(ii =>
                            {
                                cols += "," + ii;
                            });
                            //tbl.Columns[i].UniqueCols = cols.Trim(',');
                            //if (tbl.Columns[i].UniqueCols.Contains(","))
                            //{
                            //    tbl.Columns[i].IsUniqueUion = true;
                            //}
                        }
                    }
                }
            }
            return tbl;
        }

        /// <summary>
        /// 获取当前数据库的用户自定义函数
        /// </summary>
        /// <returns></returns>
        public List<Func> GetFuncs()
        {
            List<Func> res = new List<Func>();
            string sql = "select 名称=name,类型=Case [TYPE] when 'FN' then '标量函数' when 'IF' then '表值函数' end ,modify_date from sys.objects where type='FN' or type='IF' order by name asc";
            DataTable dt = GetDataTable(sql);
            if (dt.Rows.Count > 0)
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    Func func = new Func
                    {
                        Name = dt.Rows[i]["名称"].ToString(),
                        Type = dt.Rows[i]["类型"].ToString(),
                        LastUpdate = dt.Rows[i]["modify_date"].ToString()
                    };
                    string sql2 = @"sp_helptext '" + func.Name + "'";
                    StringBuilder sb = new StringBuilder();
                    DataTable dt2 = GetDataTable(sql2);
                    if (dt2.Rows.Count > 0)
                    {
                        for (int ii = 0; ii < dt.Rows.Count; ii++)
                        {
                            sb.AppendLine(dt.Rows[i][0].ToString());
                        }
                    }
                    func.CreateSql = sb.ToString();
                    res.Add(func);
                }
            }
            return res;
        }

        /// <summary>
        /// 获取当前数据库的用户自定义存储过程
        /// </summary>
        /// <returns></returns>
        public List<Proc> GetProcs()
        {
            List<Proc> res = new List<Proc>();
            string sql = @"select p.proname 名称,prosrc 创建语句 from pg_proc p where p.pronamespace in(select oid from pg_namespace where pg_namespace.nspname='public') order by proname";
            DataTable dt = GetDataTable(sql);
            if (dt.Rows.Count > 0)
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    Proc proc = new Proc
                    {
                        Name = dt.Rows[i]["名称"].ToString()
                    };
                    proc.CreateSql = dt.Rows[i]["创建语句"].ToString();


                    //List<Procparam> pp = new List<Procparam>();
                    //DataTable dt1 = GetSchema("ProcedureParameters");
                    //if (dt1 != null) {

                    //    foreach (DataRow _DataRowItem1 in dt1.Rows)
                    //    {
                    //        Procparam p = new Procparam();
                    //        p.Name = _DataRowItem1["ARGNAMES"].ToString();
                    //        pp.Add(p);
                    //    }
                    //    proc.Param = pp;

                    //}


                    res.Add(proc);
                }
            }
            return res;
        }

        /// <summary>
        /// 批量获得指定表的表结构说明
        /// </summary>
        /// <param name="tableNames">表名集合</param>
        /// <returns></returns>
        public List<TableStruct> GetTableStructs(List<string> tableNames)
        {
            List<TableStruct> res = new List<TableStruct>();
            tableNames.ForEach(i =>
            {
                res.Add(GetTableStruct(i));
            });
            return res;
        }
    }
}