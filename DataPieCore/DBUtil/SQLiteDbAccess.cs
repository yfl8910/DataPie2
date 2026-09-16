using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Text;
using System.Linq;

namespace DBUtil
{
    public partial class SQLiteDbAccess : IDbAccess
    {
        public bool IsKeepConnect { set; get; }
        public IDbTransaction tran { set; get; }
        public string ConnectionString { get; set; }
        public IDbConnection conn { set; get; }
        public DataBaseType DataBaseType { get; set; }

        public bool IsOpen { set; get; }

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

        public bool IsTran { set; get; }

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
            return new SQLiteParameter();
        }

        /// <summary>
        /// 创建具有名称和值的参数
        /// </summary>
        /// <returns>针对当前数据库类型的参数对象</returns>
        public IDbDataParameter CreatePara(string name, object value)
        {
            return new SQLiteParameter(name, value);
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
                using SQLiteCommand cmd = new SQLiteCommand(strSql, (SQLiteConnection)conn);
                if (IsTran)
                {
                    cmd.Transaction = (SQLiteTransaction)tran;
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
                using SQLiteCommand cmd = new SQLiteCommand
                {
                    Connection = (SQLiteConnection)conn
                };
                if (IsTran)
                {
                    cmd.Transaction = (SQLiteTransaction)tran;
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
                using SQLiteCommand cmd = new SQLiteCommand(strSql, (SQLiteConnection)conn);
                if (IsTran)
                {
                    cmd.Transaction = (SQLiteTransaction)tran;
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
            return OwnedDataReader.Open(this, strSql, null, 30);
        }

        public IDataReader GetDataReader(string strSql, IDbDataParameter[] paraArr)
        {
            return OwnedDataReader.Open(this, strSql, paraArr, 30);
        }
        /// <summary>
        /// 返回查询结果的数据集
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回的查询结果集</returns>
        public DataSet GetDataSet(string strSql) => GetDataSet(strSql, null);

        public DataSet GetDataSet(string strSql, IDbDataParameter[] paraArr)
        {
            using var command = new SQLiteCommand(strSql, (SQLiteConnection)conn);
            if (IsTran) command.Transaction = (SQLiteTransaction)tran;
            using var adapter = new SQLiteDataAdapter(command);
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
                if (!IsTran && !IsKeepConnect) conn.Close();
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
            string sql = string.Format("{0} {1} limit {2} offset {3}", selectSql, strOrder, PageSize, (PageIndex - 1) * PageSize);
            return sql;
        }

        /// <summary>
        /// 实现释放资源的方法
        /// </summary>
        public void Dispose()
        {
            try { tran?.Dispose(); }
            finally
            {
                conn?.Dispose();
                IsOpen = false;
                IsTran = false;
            }
        }
        /// <summary>
        /// 根据当前的数据库类型和连接字符串创建一个新的数据库操作对象
        /// </summary>
        /// <returns></returns>
        public IDbAccess CreateNewIDB()
        {
            return IDBFactory.CreateIDB(ConnectionString, DataBaseType);
        }

        public bool BulkInsert(string tableName, IDataReader reader)
        {
            return BulkInsertReader(tableName, reader, null);
        }

        List<string> IDbAccess.ShowViews() => ShowViews();

        public int TruncateTable(string tableName)
        {
            return ExecuteSql("DELETE FROM " + QuoteIdentifier(tableName));
        }

        private static string QuoteIdentifier(string name) => "\"" + name.Replace("\"", "\"\"") + "\"";

        private bool BulkInsertReader(string tableName, IDataReader reader, IList<string> columns)
        {
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException(nameof(tableName));
            if (reader == null) throw new ArgumentNullException(nameof(reader));
            var names = columns ?? Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();
            if (names.Count == 0) throw new ArgumentException("No columns to import.");
            var ordinals = names.Select(reader.GetOrdinal).ToArray();
            if (ordinals.Any(i => i < 0)) throw new ArgumentException("An import column is missing.");
            using var connection = new SQLiteConnection(ConnectionString);
            connection.Open();
            using var transaction = connection.BeginTransaction();
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "INSERT INTO " + QuoteIdentifier(tableName) + " (" +
                string.Join(",", names.Select(QuoteIdentifier)) + ") VALUES (" +
                string.Join(",", Enumerable.Range(0, names.Count).Select(i => "@p" + i)) + ")";
            var parameters = new SQLiteParameter[names.Count];
            for (int i = 0; i < parameters.Length; i++)
            {
                parameters[i] = new SQLiteParameter("@p" + i);
                command.Parameters.Add(parameters[i]);
            }
            command.Prepare();
            while (reader.Read())
            {
                for (int i = 0; i < parameters.Length; i++)
                    parameters[i].Value = reader.GetValue(ordinals[i]) ?? DBNull.Value;
                command.ExecuteNonQuery();
            }
            transaction.Commit();
            return true;
        }

        public bool BulkInsert(string tableName, DataTable dt)
        {
            if (dt == null) throw new ArgumentNullException(nameof(dt));
            using var reader = dt.CreateDataReader();
            return BulkInsert(tableName, reader);
        }

        public bool BulkInsert(string tableName, DataTable dt, IList<string> maplist)
        {
            if (dt == null) throw new ArgumentNullException(nameof(dt));
            if (maplist == null) throw new ArgumentNullException(nameof(maplist));
            using var reader = dt.CreateDataReader();
            return BulkInsertReader(tableName, reader, maplist);
        }
        public int RunProcedure(string storedProcName)
        {
            throw new NotImplementedException();
        }

        public int RunProcedure(string storedProcName, IDataParameter[] parameters, out int rowsAffected)
        {
            throw new NotImplementedException();
        }

        public IDataReader RunProcedure(string storedProcName, IDataParameter[] parameters)
        {
            throw new NotImplementedException();
        }

        public DataSet RunProcedure(string storedProcName, IDataParameter[] parameters, string tableName)
        {
            throw new NotImplementedException();
        }
    }
}