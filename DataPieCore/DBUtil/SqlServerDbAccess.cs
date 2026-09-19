using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using DataPieCore;

namespace DBUtil
{
    /// <summary>
    /// SqlServer数据库访问对象
    /// </summary>
    public partial class SqlServerDbAccess : IDbAccess
    {
        /// <summary>
        /// 是否保持连接打开
        /// </summary>
        public bool IsKeepConnect { set; get; }

        /// <summary>
        /// 连接字符串
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// 连接对象
        /// </summary>
        public IDbConnection conn { set; get; }

        /// <summary>
        /// 数据库类型
        /// </summary>
        public DataBaseType DataBaseType { get; set; }

        /// <summary>
        /// 连接是否打开
        /// </summary>
        public bool IsOpen { set; get; }



        /// <summary>
        /// 执行sql语句
        /// </summary>
        /// <param name="strSql">要执行的sql语句</param>
        /// <returns>受影响的行数</returns>
        public int ExecuteSql(string strSql)
        {
            try
            {
                using SqlCommand cmd = new SqlCommand(strSql, (SqlConnection)conn)
                {
                    CommandTimeout = 1000
                };
                    

                if (conn.State != ConnectionState.Open) conn.Open();
                IsOpen = true;

                return cmd.ExecuteNonQuery();
            }
            finally
            {
                if (!IsKeepConnect)
                {
                    try { conn?.Close(); } catch { }
                }
                IsOpen = conn?.State == ConnectionState.Open;
            }
        }



        /// <summary>
        /// 获取阅读器
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回阅读器</returns>
        public IDataReader GetDataReader(string strSql)
        {
            return OwnedDataReader.Open(this, strSql, null, 10000);
        }
        /// <summary>
        /// 返回查询结果的数据表
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回的查询数据表</returns>
        public DataTable GetDataTable(string strSql)
        {
            using var command = new SqlCommand(strSql, (SqlConnection)conn);
            using var adapter = new SqlDataAdapter(command);
            var table = new DataTable("Table");
            try
            {
                if (conn.State != ConnectionState.Open) conn.Open();
                IsOpen = true;
                adapter.Fill(table);
                if (table.Columns.Count > 0) return table;
                table.Dispose();
                return null;
            }
            catch { table.Dispose(); throw; }
            finally
            {
                if (!IsKeepConnect) conn.Close();
                IsOpen = conn.State == ConnectionState.Open;
            }
        }

        /// <summary>
        /// 实现释放资源的方法
        /// </summary>
        public void Dispose()
        {
            conn?.Dispose();
            IsOpen = false;
        }
        /// <summary>
        /// 根据当前的数据库类型和连接字符串创建一个新的数据库操作对象
        /// </summary>
        /// <returns></returns>
        public IDbAccess CreateNewAccess()
        {
            return DbAccessFactory.Create(ConnectionString, DataBaseType);
        }

  

        #region 批量插入操作


        public bool BulkInsert(string tableName, IDataReader reader)
        {
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException(nameof(tableName));
            if (reader == null) throw new ArgumentNullException(nameof(reader));
            string destination = SqlQueryBuilder.QuoteSqlServerTableName(tableName);

            using SqlConnection connection = new SqlConnection(ConnectionString);
            connection.Open();
            using SqlBulkCopy bulkCopy = new SqlBulkCopy(connection)
            {
                DestinationTableName = destination
            };

            // Discover columns using the SAME connection used for bulk copy
            var destinationColumns = new HashSet<string>(StringComparer.Ordinal);
            using (var schemaCmd = new SqlCommand($"SELECT TOP (0) * FROM {destination}", connection))
            using (var schemaReader = schemaCmd.ExecuteReader())
            {
                for (int i = 0; i < schemaReader.FieldCount; i++)
                    destinationColumns.Add(schemaReader.GetName(i));
            }

            AddColumnMappings(bulkCopy, reader, destinationColumns);

            bulkCopy.WriteToServer(reader);
            return true;
        }

        internal static void AddColumnMappings(SqlBulkCopy bulkCopy, IDataReader reader, HashSet<string> destinationColumns)
        {
            for (int i = 0; i < reader.FieldCount; i++)
            {
                string name = reader.GetName(i);
                if (destinationColumns.Remove(name)) bulkCopy.ColumnMappings.Add(i, name);
            }
            if (bulkCopy.ColumnMappings.Count == 0)
                throw new InvalidOperationException("No matching columns were found in the destination table.");
        }

        public int TruncateTable(string tableName)
            => ExecuteSql("TRUNCATE TABLE " + SqlQueryBuilder.QuoteSqlServerTableName(tableName));

        #endregion 批量插入操作

        #region 存储过程操作

        /// <summary>
        /// 执行存储过程，返回影响行数
        /// </summary>
        public int RunProcedure(string storedProcName)
        {
            using var connection = new SqlConnection(ConnectionString);
            using var command = new SqlCommand(storedProcName, connection)
            {
                CommandType = CommandType.StoredProcedure,
                CommandTimeout = 100000
            };
            connection.Open();
            return command.ExecuteNonQuery();
        }


        #endregion 存储过程操作
    }
}
