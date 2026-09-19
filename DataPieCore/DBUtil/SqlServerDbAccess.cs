using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

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
        /// 创建具有名称和值的参数
        /// </summary>
        /// <returns>针对当前数据库类型的参数对象</returns>
        public IDbDataParameter CreatePara(string name, object value)
        {
            return new SqlParameter(name, value);
        }



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
                    

                if (!IsOpen)
                {
                    conn.Open();
                    IsOpen = true;
                }

                return cmd.ExecuteNonQuery();
            }
            finally
            {
                if (!IsKeepConnect)
                {
                    try { conn?.Close(); } catch { }
                    this.IsOpen = false;
                }
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

            using SqlConnection connection = new SqlConnection(ConnectionString);
            connection.Open();
            using SqlBulkCopy bulkCopy = new SqlBulkCopy(connection)
            {
                DestinationTableName = tableName
            };

            // Discover columns using the SAME connection used for bulk copy
            var columns2 = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToList();
            List<string> columns1;
            using (var schemaCmd = new SqlCommand($"SELECT TOP (0) * FROM [{tableName}]", connection))
            using (var schemaReader = schemaCmd.ExecuteReader())
            {
                columns1 = Enumerable.Range(0, schemaReader.FieldCount).Select(schemaReader.GetName).ToList();
            }

            var newcolumns = columns1.Intersect(columns2);
            foreach (var cl in newcolumns)
            {
                bulkCopy.ColumnMappings.Add(cl, cl);
            }

            bulkCopy.WriteToServer(reader);
            return true;
        }

        public bool BulkInsert(string tableName, DataTable dt, IList<string> maplist)
        {
            using SqlConnection connection = new SqlConnection(ConnectionString);
            connection.Open();
            using SqlBulkCopy bulkCopy = new SqlBulkCopy(connection)
            {
                DestinationTableName = tableName
            };

            foreach (string a in maplist)
            {
                bulkCopy.ColumnMappings.Add(a, a);
            }

            bulkCopy.WriteToServer(dt);
            return true;
        }

        public bool BulkInsert(string tableName, DataTable dt)
        {
            if (dt == null) throw new ArgumentNullException(nameof(dt));
            using var reader = dt.CreateDataReader();
            return BulkInsert(tableName, reader);
        }

        public int TruncateTable(string TableName)
        {
            return ExecuteSql("TRUNCATE TABLE   [" + TableName + "]");
        }

        #endregion 批量插入操作

        #region 存储过程操作

        /// <summary>
        /// 执行存储过程，返回SqlDataReader ( 注意：调用该方法后，一定要对SqlDataReader进行Close )
        /// </summary>
        /// <param name="storedProcName">存储过程名</param>
        /// <param name="parameters">存储过程参数</param>
        /// <returns>SqlDataReader</returns>
        public IDataReader RunProcedure(string storedProcName, IDataParameter[] parameters)
        {
            var boundParameters = parameters?.Cast<IDbDataParameter>().ToArray();
            return OwnedDataReader.Open(CreateNewAccess(), storedProcName,
                boundParameters, 100000, CommandType.StoredProcedure, true);
        }

        /// <summary>
        /// 执行存储过程，返回影响行数
        /// </summary>
        public int RunProcedure(string storedProcName)
        {
            using SqlConnection connection = new SqlConnection(ConnectionString);
            int result;
            connection.Open();
            using SqlCommand command = BuildIntCommand(connection, storedProcName, null);
            result = command.ExecuteNonQuery();
            return result;
        }

        /// <summary>
        /// 执行存储过程
        /// </summary>
        /// <param name="storedProcName">存储过程名</param>
        /// <param name="parameters">存储过程参数</param>
        /// <param name="tableName">DataSet结果中的表名</param>
        /// <returns>DataSet</returns>
        public DataSet RunProcedure(string storedProcName, IDataParameter[] parameters, string tableName)
        {
            using SqlConnection connection = new SqlConnection(ConnectionString);
            DataSet dataSet = new DataSet();
            connection.Open();
            using var command = BuildQueryCommand(connection, storedProcName, parameters);
            using var sqlDA = new SqlDataAdapter(command);
            sqlDA.Fill(dataSet, tableName);
            return dataSet;
        }


        /// <summary>
        /// 构建 SqlCommand 对象(用来返回一个结果集，而不是一个整数值)
        /// </summary>
        /// <param name="connection">数据库连接</param>
        /// <param name="storedProcName">存储过程名</param>
        /// <param name="parameters">存储过程参数</param>
        /// <returns>SqlCommand</returns>
        private SqlCommand BuildQueryCommand(SqlConnection connection, string storedProcName, IDataParameter[] parameters)
        {
            SqlCommand command = new SqlCommand(storedProcName, connection)
            {
                CommandType = CommandType.StoredProcedure,
                CommandTimeout = 100000
            };

            if (parameters != null)
            {
                foreach (SqlParameter parameter in parameters)
                {
                    // 检查未分配值的输出参数,将其分配以DBNull.Value.
                    if ((parameter.Direction == ParameterDirection.InputOutput || parameter.Direction == ParameterDirection.Input) &&
                        (parameter.Value == null))
                    {
                        parameter.Value = DBNull.Value;
                    }
                    command.Parameters.Add(parameter);
                }
            }

            return command;
        }

        /// <summary>
        /// 执行存储过程，返回影响的行数
        /// </summary>
        /// <param name="storedProcName">存储过程名</param>
        /// <param name="parameters">存储过程参数</param>
        /// <param name="rowsAffected">影响的行数</param>
        /// <returns></returns>
        public int RunProcedure(string storedProcName, IDataParameter[] parameters, out int rowsAffected)
        {
            using SqlConnection connection = new SqlConnection(ConnectionString);
            int result;
            connection.Open();
            using SqlCommand command = BuildIntCommand(connection, storedProcName, parameters);
            rowsAffected = command.ExecuteNonQuery();
            result = (int)command.Parameters["ReturnValue"].Value;
            return result;
        }

        /// <summary>
        /// 创建 SqlCommand 对象实例(用来返回一个整数值)
        /// </summary>
        /// <param name="storedProcName">存储过程名</param>
        /// <param name="parameters">存储过程参数</param>
        /// <returns>SqlCommand 对象实例</returns>
        private SqlCommand BuildIntCommand(SqlConnection connection, string storedProcName, IDataParameter[] parameters)
        {
            SqlCommand command = BuildQueryCommand(connection, storedProcName, parameters);
            command.Parameters.Add(new SqlParameter("ReturnValue",
                SqlDbType.Int, 4, ParameterDirection.ReturnValue,
                false, 0, 0, string.Empty, DataRowVersion.Default, null));
            return command;
        }

        #endregion 存储过程操作
    }
}
