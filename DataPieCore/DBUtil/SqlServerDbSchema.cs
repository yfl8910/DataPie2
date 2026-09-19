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
                DbProcs = GetProcs()
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
            var columnsByTable = allColumns.AsEnumerable().ToLookup(r => Convert.ToInt32(r["TableId"]));
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
                    Columns = columns
                });
            }
            return tables;
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
