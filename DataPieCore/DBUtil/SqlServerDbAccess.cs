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
        /// 事务管理对象
        /// </summary>
        public IDbTransaction tran { set; get; }

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
        /// 是否开启了事务
        /// </summary>

        public bool IsTran { set; get; }

        /// <summary>
        /// 打开连接测试
        /// </summary>
        /// <returns></returns>
        public Result OpenTest()
        {
            try
            {
                conn.Open();
                conn.Close();
                return new Result()
                {
                    Success = true
                };
            }
            catch (Exception ex)
            {
                return new Result()
                {
                    Success = false,
                    Data = ex.ToString()
                };
            }
        }

        /// <summary>
        /// 当前数据库使用的参数的前缀符号
        /// </summary>
        public string paraPrefix { get { return "@"; } }

        /// <summary>
        /// 创建参数
        /// </summary>
        /// <returns></returns>
        public IDbDataParameter CreatePara()
        {
            return new SqlParameter();
        }

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
                SqlCommand cmd = new SqlCommand(strSql, (SqlConnection)conn);
                cmd.CommandTimeout = 1000;

                if (IsTran)
                {
                    cmd.Transaction = (SqlTransaction)tran;
                }
                if (!IsOpen)
                {
                    conn.Open();
                    IsOpen = true;
                }
                int r = cmd.ExecuteNonQuery();
                return r;
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                if (!IsTran && !IsKeepConnect)
                {
                    conn.Close();
                    this.IsOpen = false;
                }
            }
        }

        /// <summary>
        /// 执行多个sql语句
        /// </summary>
        /// <param name="strSql">多个SQL语句的数组</param>
        public void ExecuteSql(string[] strSql)
        {
            try
            {
                SqlCommand cmd = new SqlCommand

                {
                    Connection = (SqlConnection)conn
                };
                if (IsTran)
                {
                    cmd.CommandTimeout = 1000;
                    cmd.Transaction = (SqlTransaction)tran;
                }
                if (!IsOpen)
                {
                    conn.Open();
                }
                foreach (string sql in strSql)
                {
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                if (!IsTran && !IsKeepConnect)
                {
                    conn.Close();
                    this.IsOpen = false;
                }
            }
        }

        /// <summary>
        /// 执行带参数的sql语句
        /// </summary>
        /// <param name="strSql">要执行的sql语句</param>
        /// <param name="paramArr">参数数组</param>
        /// <returns></returns>
        public int ExecuteSql(string strSql, IDataParameter[] paramArr)
        {
            try
            {
                SqlCommand cmd = new SqlCommand(strSql, (SqlConnection)conn);
                cmd.CommandTimeout = 1000;

                if (IsTran)
                {
                    cmd.Transaction = (SqlTransaction)tran;
                }
                cmd.Parameters.AddRange(paramArr);
                if (!IsOpen)
                {
                    conn.Open();
                    IsOpen = true;
                }
                int r = cmd.ExecuteNonQuery();
                cmd.Parameters.Clear();
                return r;
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                if (!IsTran && !IsKeepConnect)
                {
                    conn.Close();
                    this.IsOpen = false;
                }
            }
        }

        /// <summary>
        /// 批量执行带参数的sql语句
        /// </summary>
        /// <param name="strSql"></param>
        /// <param name="paraArrs"></param>
        public void ExecuteSql(string[] strSql, IDataParameter[][] paraArrs)
        {
            for (int i = 0; i < strSql.Length; i++)
            {
                ExecuteSql(strSql[i], paraArrs[i]);
            }
        }



        /// <summary>
        /// 获取阅读器
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回阅读器</returns>
        public IDataReader GetDataReader(string strSql)
        {
            SqlCommand cmd = new SqlCommand(strSql, (SqlConnection)conn)
            {
                CommandTimeout = 10000
            };
            if (IsTran)
            {
                cmd.Transaction = (SqlTransaction)tran;
            }
            try
            {
                conn.Open();
                SqlDataReader myReader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                return myReader;
            }
            catch (System.Data.SqlClient.SqlException e)
            {
                throw e;
            }
        }

        /// <summary>
        /// 获取阅读器
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回阅读器</returns>
        public IDataReader GetDataReader(string strSql, IDbDataParameter[] paraArr)
        {
            SqlCommand cmd = new SqlCommand(strSql, (SqlConnection)conn);
            cmd.Parameters.AddRange(paraArr);
            if (IsTran)
            {
                cmd.Transaction = (SqlTransaction)tran;
            }
            if (!IsOpen)
            {
                conn.Open();
                IsOpen = true;
            }
            IDataReader reader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
            cmd.Parameters.Clear();
            return reader;
        }

        /// <summary>
        /// 返回查询结果的数据集
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回的查询结果集</returns>
        public DataSet GetDataSet(string strSql)
        {
            SqlCommand cmd = new SqlCommand(strSql, (SqlConnection)conn);
            if (IsTran)
            {
                cmd.Transaction = (SqlTransaction)tran;
            }
            if (!IsOpen)
            {
                conn.Open();
                IsOpen = true;
            }
            SqlDataAdapter adp = new SqlDataAdapter(cmd);
            DataSet set = new DataSet();
            adp.Fill(set);
            if (!IsTran && !IsKeepConnect)
            {
                conn.Close();
                this.IsOpen = false;
            }
            return set;
        }

        /// <summary>
        /// 返回查询结果的数据集
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="paraArr">SQL语句中的参数集合</param>
        /// <returns>返回的查询结果集</returns>
        public DataSet GetDataSet(string strSql, IDbDataParameter[] paraArr)
        {
            SqlCommand cmd = new SqlCommand(strSql, (SqlConnection)conn);
            cmd.Parameters.AddRange(paraArr);
            if (IsTran)
            {
                cmd.Transaction = (SqlTransaction)tran;
            }
            if (!IsOpen)
            {
                conn.Open();
                IsOpen = true;
            }
            SqlDataAdapter adp = new SqlDataAdapter(cmd);
            DataSet set = new DataSet();
            adp.Fill(set);
            cmd.Parameters.Clear();
            if (!IsTran && !IsKeepConnect)
            {
                conn.Close();
                this.IsOpen = false;
            }
            return set;
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
        /// 返回的查询数据表
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="paraArr">SQL语句中的参数集合</param>
        /// <returns>返回的查询数据表</returns>
        public DataTable GetDataTable(string strSql, IDbDataParameter[] paraArr)
        {
            DataSet ds = GetDataSet(strSql, paraArr);
            if (ds.Tables.Count > 0)
            {
                DataTable dt = ds.Tables[0];
                ds.Tables.Remove(dt);
                return dt;
            }
            return null;
        }

        /// <summary>
        /// 开启事务
        /// </summary>
        public void BeginTrans()
        {
            if (!IsOpen)
            {
                conn.Open();
                IsOpen = true;
            }
            if (IsTran)
            {
                tran.Commit();
            }
            tran = conn.BeginTransaction();
            IsTran = true;
        }

        /// <summary>
        /// 提交事务
        /// </summary>
        public void Commit()
        {
            tran.Commit();
        }

        /// <summary>
        /// 回滚事务
        /// </summary>
        public void Rollback()
        {
            tran.Rollback();
        }


        /// <summary>
        /// 获得分页的查询语句
        /// </summary>
        /// <param name="selectSql">查询sql如:select name,id from test where id>5</param>
        /// <param name="strOrder">排序字句如:order by id desc</param>
        /// <param name="PageSize">页面大小</param>
        /// <param name="PageIndex">页面索引从1开始</param>
        /// <returns>经过分页的sql语句</returns>
        public string GetSqlForPageSize(string selectSql, string strOrder, int PageSize, int PageIndex)
        {
            string sql = string.Format("select * from (select *,ROW_NUMBER() OVER({0}) as RNO__ from ({1}) as inner__ ) outer__ WHERE outer__.RNO__ BETWEEN ({2}*{3}+1) AND ({4}*{5})", strOrder, selectSql, PageIndex - 1, PageSize, PageIndex, PageSize);
            return sql;
        }

        /// <summary>
        /// 实现释放资源的方法
        /// </summary>
        public void Dispose()
        {
            try
            {
                if (this.conn.State != ConnectionState.Closed)
                {
                    this.conn.Close();
                    this.IsOpen = false;
                }
            }
            catch { }
        }

        /// <summary>
        /// 根据当前的数据库类型和连接字符串创建一个新的数据库操作对象
        /// </summary>
        /// <returns></returns>
        public IDbAccess CreateNewIDB()
        {
            return IDBFactory.CreateIDB(ConnectionString, DataBaseType);
        }

  

        #region 批量插入操作

        public bool BulkInsert(string tableName, IDataReader reader)
        {
            using SqlConnection connection = new SqlConnection(ConnectionString);
            connection.Open();
            using SqlBulkCopy bulkCopy = new SqlBulkCopy(connection)
            {
                DestinationTableName = tableName
            };

            DataTable dt = GetDataTable("select * from " + tableName + " where 1=2");

            List<string> columns1 = new List<string>();

            foreach (DataColumn col in dt.Columns)
            {
                columns1.Add(col.ColumnName);
            }

            var columns2 = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToList();

            var newcolumns = columns1.Intersect(columns2);

            foreach (var cl in newcolumns)
            {
                bulkCopy.ColumnMappings.Add(cl, cl);
            }

            try
            {
                bulkCopy.WriteToServer(reader);
                connection.Close();
                return true;
            }
            catch (Exception e)
            {
                throw e;
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