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
            catch (Exception)
            {
                throw;
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
        /// 返回查询结果的数据集
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回的查询结果集</returns>
        private DataSet GetDataSet(string strSql) => GetDataSet(strSql, null);

        private DataSet GetDataSet(string strSql, IDbDataParameter[] paraArr)
        {
            using var command = new SqlCommand(strSql, (SqlConnection)conn);
            using var adapter = new SqlDataAdapter(command);
            var result = new DataSet();
            try
            {
                if (paraArr != null) command.Parameters.AddRange(paraArr);
                if (conn.State != ConnectionState.Open) conn.Open();
                IsOpen = true;
                adapter.Fill(result);
                return result;
            }
            catch { result.Dispose(); throw; }
            finally
            {
                command.Parameters.Clear();
                if (!IsKeepConnect) conn.Close();
                IsOpen = conn.State == ConnectionState.Open;
            }
        }
        /// <summary>
        /// 返回查询结果的数据表
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回的查询数据表</returns>
        public DataTable GetDataTable(string strSql)
        {
            DataSet ds = GetDataSet(strSql);
            if (ds.Tables.Count > 0)
            {
                DataTable dt = ds.Tables[0];
                ds.Tables.Remove(dt);
                return dt;
            }
            return null;
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

        // Helper to get column names for a table using the provided connection (avoids using this.conn)
        private static List<string> GetTableColumnsUsingConnection(SqlConnection connection, string tableName)
        {
            if (connection == null) return new List<string>();
            string sql = $"SELECT TOP (0) * FROM [{tableName}]";
            using var cmd = new SqlCommand(sql, connection);
            using var reader = cmd.ExecuteReader();
            var names = new List<string>(reader.FieldCount);
            for (int i = 0; i < reader.FieldCount; i++)
            {
                names.Add(reader.GetName(i));
            }
            return names;
        }

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

            try
            {
                bulkCopy.WriteToServer(reader);
                return true;
            }
            catch (Exception)
            {
                throw;
            }
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

            try
            {
                bulkCopy.WriteToServer(dt);
                return true;
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                connection.Close();
            }
        }

        public bool BulkInsert(string tableName, DataTable dt)
        {
            using SqlConnection connection = new SqlConnection(ConnectionString);
            connection.Open();
            using SqlBulkCopy bulkCopy = new SqlBulkCopy(connection)
            {
                DestinationTableName = tableName
            };

            DataTable dt2 = GetDataTable("select * from " + tableName + " where 1=2");

            List<string> cols = new List<string>();

            foreach (DataColumn col in dt2.Columns)
            {
                cols.Add(col.ColumnName);
            }

            //仅仅导入列名一致的表
            foreach (DataColumn dc in dt.Columns)
            {
                if (cols.Contains(dc.ColumnName))
                {
                    bulkCopy.ColumnMappings.Add(dc.ColumnName, dc.ColumnName);
                }
            }

            try
            {
                bulkCopy.WriteToServer(dt);

                return true;
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                connection.Close();
            }
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
            SqlConnection connection = new SqlConnection(ConnectionString);
            SqlDataReader returnReader;
            connection.Open();
            SqlCommand command = BuildQueryCommand(connection, storedProcName, parameters);
            command.CommandType = CommandType.StoredProcedure;
            returnReader = command.ExecuteReader(CommandBehavior.CloseConnection);
            return returnReader;
        }

        /// <summary>
        /// 执行存储过程，返回影响行数
        /// </summary>
        public int RunProcedure(string storedProcName)
        {
            using SqlConnection connection = new SqlConnection(ConnectionString);
            int result;
            connection.Open();
            SqlCommand command = BuildIntCommand(connection, storedProcName, null);
            command.CommandTimeout = 100000;
            result = command.ExecuteNonQuery();
            connection.Close();
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
            SqlDataAdapter sqlDA = new SqlDataAdapter
            {
                SelectCommand = BuildQueryCommand(connection, storedProcName, parameters)
            };
            sqlDA.Fill(dataSet, tableName);
            connection.Close();
            return dataSet;
        }

        public DataSet RunProcedure(string storedProcName, IDataParameter[] parameters, string tableName, int Times)
        {
            using SqlConnection connection = new SqlConnection(ConnectionString);
            DataSet dataSet = new DataSet();
            connection.Open();
            SqlDataAdapter sqlDA = new SqlDataAdapter
            {
                SelectCommand = BuildQueryCommand(connection, storedProcName, parameters)
            };
            sqlDA.SelectCommand.CommandTimeout = Times;
            sqlDA.Fill(dataSet, tableName);
            connection.Close();
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
                CommandType = CommandType.StoredProcedure
            };

            command.CommandTimeout = 100000;
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
            SqlCommand command = BuildIntCommand(connection, storedProcName, parameters);
            command.CommandTimeout = 100000;
            rowsAffected = command.ExecuteNonQuery();
            result = (int)command.Parameters["ReturnValue"].Value;
            connection.Close();
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
            command.CommandTimeout = 100000;
            command.Parameters.Add(new SqlParameter("ReturnValue",
                SqlDbType.Int, 4, ParameterDirection.ReturnValue,
                false, 0, 0, string.Empty, DataRowVersion.Default, null));
            return command;
        }

        #endregion 存储过程操作
    }
}
