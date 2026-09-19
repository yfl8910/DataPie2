using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Text;
using System.Linq;
using DataPieCore;

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

        public bool IsTran { set; get; }

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
        /// 获取阅读器
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回阅读器</returns>
        public IDataReader GetDataReader(string strSql)
        {
            return OwnedDataReader.Open(this, strSql, null, 30);
        }
        /// <summary>
        /// 返回查询结果的数据集
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回的查询结果集</returns>
        private DataSet GetDataSet(string strSql) => GetDataSet(strSql, null);

        private DataSet GetDataSet(string strSql, IDbDataParameter[] paraArr)
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
        public IDbAccess CreateNewAccess()
        {
            return DbAccessFactory.Create(ConnectionString, DataBaseType);
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
            return RunProcedure(storedProcName, Array.Empty<IDataParameter>(), out _);
        }

        public int RunProcedure(string storedProcName, IDataParameter[] parameters, out int rowsAffected)
        {
            var script = SQLiteProcedureScripts.Load(ConnectionString, storedProcName);
            var bound = SQLiteProcedureScripts.Bind(script, parameters);
            try
            {
                if (conn.State != ConnectionState.Open) conn.Open();
                IsOpen = true;
                using var transaction = IsTran ? null : ((SQLiteConnection)conn).BeginTransaction();
                using var command = new SQLiteCommand(script.Sql, (SQLiteConnection)conn)
                {
                    Transaction = IsTran ? (SQLiteTransaction)tran : transaction,
                    CommandTimeout = 600
                };
                command.Parameters.AddRange(bound.Cast<SQLiteParameter>().ToArray());
                rowsAffected = command.ExecuteNonQuery();
                transaction?.Commit();
                return rowsAffected;
            }
            finally
            {
                if (!IsTran && !IsKeepConnect) conn.Close();
                IsOpen = conn.State == ConnectionState.Open;
            }
        }

        public IDataReader RunProcedure(string storedProcName, IDataParameter[] parameters)
        {
            var script = SQLiteProcedureScripts.Load(ConnectionString, storedProcName);
            if (!script.Metadata.ReadOnly)
                throw new NotSupportedException("Use the non-query RunProcedure overload for scripts that modify data.");
            return OwnedDataReader.Open(this, script.Sql, SQLiteProcedureScripts.Bind(script, parameters), 600);
        }

        public DataSet RunProcedure(string storedProcName, IDataParameter[] parameters, string tableName)
        {
            var script = SQLiteProcedureScripts.Load(ConnectionString, storedProcName);
            if (!script.Metadata.ReadOnly)
                throw new NotSupportedException("Use the non-query RunProcedure overload for scripts that modify data.");
            using var command = new SQLiteCommand(script.Sql, (SQLiteConnection)conn) { CommandTimeout = 600 };
            if (IsTran) command.Transaction = (SQLiteTransaction)tran;
            command.Parameters.AddRange(SQLiteProcedureScripts.Bind(script, parameters).Cast<SQLiteParameter>().ToArray());
            using var adapter = new SQLiteDataAdapter(command);
            var result = new DataSet();
            adapter.Fill(result, tableName);
            return result;
        }
    }
}
