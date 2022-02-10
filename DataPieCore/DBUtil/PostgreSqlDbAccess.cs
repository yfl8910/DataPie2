using Npgsql;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace DBUtil
{
    public partial class PostgreSqlDbAccess : IDbAccess
    {
        public bool IsKeepConnect { set; get; }
        public IDbTransaction tran { set; get; }
        public string ConnectionString { get; set; }
        public IDbConnection conn { set; get; }
        public DataBaseType DataBaseType { get; set; }

        public bool IsOpen { set; get; }

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
        public string paraPrefix { get { return ":"; } }

        /// <summary>
        /// 创建参数
        /// </summary>
        /// <returns></returns>
        public IDbDataParameter CreatePara()
        {
            return new Npgsql.NpgsqlParameter();
        }

        /// <summary>
        /// 创建具有名称和值的参数
        /// </summary>
        /// <returns>针对当前数据库类型的参数对象</returns>
        public IDbDataParameter CreatePara(string name, object value)
        {
            return new NpgsqlParameter(name, value);
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
                NpgsqlCommand cmd = new NpgsqlCommand(strSql, (NpgsqlConnection)conn);
                if (IsTran)
                {
                    cmd.Transaction = (NpgsqlTransaction)tran;
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
                NpgsqlCommand cmd = new NpgsqlCommand
                {
                    Connection = (NpgsqlConnection)conn
                };
                if (IsTran)
                {
                    cmd.Transaction = (NpgsqlTransaction)tran;
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
                NpgsqlCommand cmd = new NpgsqlCommand(strSql, (NpgsqlConnection)conn);
                if (IsTran)
                {
                    cmd.Transaction = (NpgsqlTransaction)tran;
                }
                cmd.Parameters.AddRange(paramArr);
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
            NpgsqlCommand cmd = new NpgsqlCommand(strSql, (NpgsqlConnection)conn);
            if (IsTran)
            {
                cmd.Transaction = (NpgsqlTransaction)tran;
            }
            if (!IsOpen)
            {
                conn.Open();
                IsOpen = true;
            }
            return cmd.ExecuteReader();
        }

        /// <summary>
        /// 获取阅读器
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回阅读器</returns>
        public IDataReader GetDataReader(string strSql, IDbDataParameter[] paraArr)
        {
            NpgsqlCommand cmd = new NpgsqlCommand(strSql, (NpgsqlConnection)conn);
            cmd.Parameters.AddRange(paraArr);
            if (IsTran)
            {
                cmd.Transaction = (NpgsqlTransaction)tran;
            }
            if (!IsOpen)
            {
                conn.Open();
                IsOpen = true;
            }
            return cmd.ExecuteReader();
        }

        /// <summary>
        /// 返回查询结果的数据集
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回的查询结果集</returns>
        public DataSet GetDataSet(string strSql)
        {
            NpgsqlCommand cmd = new NpgsqlCommand(strSql, (NpgsqlConnection)conn);
            if (IsTran)
            {
                cmd.Transaction = (NpgsqlTransaction)tran;
            }
            if (!IsOpen)
            {
                conn.Open();
                IsOpen = true;
            }
            NpgsqlDataAdapter adp = new NpgsqlDataAdapter(cmd);
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
            NpgsqlCommand cmd = new NpgsqlCommand(strSql, (NpgsqlConnection)conn);
            cmd.Parameters.AddRange(paraArr);
            if (IsTran)
            {
                cmd.Transaction = (NpgsqlTransaction)tran;
            }
            if (!IsOpen)
            {
                conn.Open();
                IsOpen = true;
            }
            NpgsqlDataAdapter adp = new NpgsqlDataAdapter(cmd);
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
            string sql = string.Format("{0} {1} limit {2} offset{3}", selectSql, strOrder, PageSize, (PageIndex - 1) * PageSize);
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

        public int TruncateTable(string TableName)
        {
            return ExecuteSql("TRUNCATE TABLE " + TableName);
        }

        private NpgsqlCommand BuildIntCommand(NpgsqlConnection connection, string storedProcName, IDataParameter[] parameters)
        {
            NpgsqlCommand command = BuildQueryCommand(connection, storedProcName, parameters);

            return command;
        }

        private NpgsqlCommand BuildQueryCommand(NpgsqlConnection connection, string storedProcName, IDataParameter[] parameters)
        {
            NpgsqlCommand command = new NpgsqlCommand(storedProcName, connection)
            {
                CommandType = CommandType.StoredProcedure
            };
            if (parameters != null)
            {
                foreach (NpgsqlParameter parameter in parameters)
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

        public int RunProcedure(string storedProcName)
        {
            using NpgsqlConnection connection = new NpgsqlConnection(ConnectionString);
            int result;
            connection.Open();
            NpgsqlCommand command = BuildIntCommand(connection, storedProcName, null);
            command.CommandTimeout = 100000;
            result = command.ExecuteNonQuery();
            connection.Close();
            return result;
        }

        public int RunProcedure(string storedProcName, IDataParameter[] parameters, out int rowsAffected)
        {
            throw new NotImplementedException();
        }

        public IDataReader RunProcedure(string storedProcName, IDataParameter[] parameters)
        {
            NpgsqlConnection connection = new NpgsqlConnection(ConnectionString);
            NpgsqlDataReader returnReader;
            connection.Open();
            NpgsqlCommand command = BuildQueryCommand(connection, storedProcName, parameters);
            command.CommandType = CommandType.StoredProcedure;
            returnReader = command.ExecuteReader(CommandBehavior.CloseConnection);
            connection.Close();
            return returnReader;
        }

        public DataSet RunProcedure(string storedProcName, IDataParameter[] parameters, string tableName)
        {
            using NpgsqlConnection connection = new NpgsqlConnection(ConnectionString);
            DataSet dataSet = new DataSet();
            connection.Open();
            NpgsqlDataAdapter sqlDA = new NpgsqlDataAdapter
            {
                SelectCommand = BuildQueryCommand(connection, storedProcName, parameters)
            };
            sqlDA.Fill(dataSet, tableName);
            connection.Close();
            return dataSet;
        }

        public bool BulkInsert(string tableName, DataTable dt, IList<string> maplist)
        {
            using NpgsqlConnection conn = new NpgsqlConnection(ConnectionString);
            conn.Open();
            NpgsqlCommand cmd = new NpgsqlCommand
            {
                Connection = conn
            };
            NpgsqlTransaction tx = conn.BeginTransaction();
            cmd.Transaction = tx;
            try
            {
                foreach (DataRow r in dt.Rows)
                {
                    cmd.CommandText = GenerateInserSql(maplist, tableName, r);
                    cmd.ExecuteNonQuery();
                }

                tx.Commit();
                conn.Close();
                return true;
            }
            catch (Exception E)
            {
                tx.Rollback();
                conn.Close();
                throw new Exception(E.Message);
            }
        }

        public bool BulkInsert(string tableName, IDataReader reader)
        {
            throw new NotImplementedException();
        }

        private string GenerateInserSql(IList<string> maplist, string TableName, DataRow row)
        {
            var names = new StringBuilder();
            var values = new StringBuilder();
            bool first = true;
            char quote = '\'';

            foreach (string c in maplist)
            {
                if (!first)
                {
                    names.Append(",");
                    values.Append(",");
                }
                names.Append(c);
                values.Append("\'" + row[c].ToString().Replace(quote.ToString(), string.Concat(quote, quote)) + "\'");
                first = false;
            }

            string sql = string.Format("INSERT INTO {0} ({1}) VALUES ({2})", TableName, names, values);
            return sql;
        }

        public bool BulkInsert(string tableName, DataTable dt)
        {
            using NpgsqlConnection conn = new NpgsqlConnection(ConnectionString);
            conn.Open();
            NpgsqlCommand cmd = new NpgsqlCommand
            {
                Connection = conn
            };
            NpgsqlTransaction tx = conn.BeginTransaction();
            cmd.Transaction = tx;
            try
            {
                IList<string> maplist = new List<string>();

                foreach (var c in dt.Columns)
                {
                    maplist.Add(c.ToString());
                }

                foreach (DataRow r in dt.Rows)
                {
                    cmd.CommandText = GenerateInserSql(maplist, tableName, r);
                    cmd.ExecuteNonQuery();
                }

                tx.Commit();
                conn.Close();
                return true;
            }
            catch (Exception E)
            {
                tx.Rollback();
                conn.Close();
                throw new Exception(E.Message);
            }
        }
    }
}