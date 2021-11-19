using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

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
        /// 根据指定日期范围生成过滤字符串
        /// </summary>
        /// <param name="dateColumn">要进行过滤的字段名称</param>
        /// <param name="minDate">最小日期</param>
        /// <param name="maxDate">最大日期</param>
        /// <param name="isMinInclude">最小日期是否包含</param>
        /// <param name="isMaxInclude">最大日期是否包含</param>
        /// <returns>返回生成的过滤字符串</returns>
        public string GetDateFilter(string dateColumn, string minDate, string maxDate, bool isMinInclude, bool isMaxInclude)
        {
            if (DateTime.TryParse(minDate, out _) && DateTime.TryParse(maxDate, out _))
            {
                string res = "";
                if (isMinInclude)
                {
                    res += " and " + dateColumn + ">='" + minDate + "'";
                }
                else
                {
                    res += " and " + dateColumn + ">'" + minDate + "'";
                }
                if (isMaxInclude)
                {
                    res += " and " + dateColumn + "<='" + maxDate + "'";
                }
                else
                {
                    res += " and " + dateColumn + "<'" + maxDate + "'";
                }
                return res;
            }
            else
            {
                throw new Exception("非正确的格式:[" + minDate + "]或[" + maxDate + "]");
            }
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
        /// 向一个表中添加一行数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="ht">列名和值得键值对</param>
        /// <returns>返回是受影响的行数</returns>
        public bool AddData(string tableName, Hashtable ht)
        {
            string insertTableOption = "";
            string insertTableValues = "";
            List<IDbDataParameter> paras = new List<IDbDataParameter>();
            foreach (System.Collections.DictionaryEntry item in ht)
            {
                insertTableOption += " " + item.Key.ToString() + ",";
                insertTableValues += "@" + item.Key.ToString() + ",";
                paras.Add(new SqlParameter()
                {
                    ParameterName = item.Key.ToString(),
                    Value = item.Value
                });
            }
            insertTableOption = insertTableOption.TrimEnd(new char[] { ',' });
            insertTableValues = insertTableValues.TrimEnd(new char[] { ',' });

            string strSql = string.Format("insert into {0} ({1}) values ({2})", tableName, insertTableOption, insertTableValues);
            return ExecuteSql(strSql, paras.ToArray()) > 0 ? true : false;
        }

        /// <summary>
        /// 向一个表中添加一行数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="dic">列名和值得键值对</param>
        /// <returns>返回是受影响的行数</returns>
        public bool AddData(string tableName, Dictionary<string, object> dic)
        {
            Hashtable ht = new Hashtable();
            foreach (var i in dic)
            {
                ht.Add(i.Key, i.Value);
            }
            return AddData(tableName, ht);
        }

        /// <summary>
        /// 根据键值表中的数据向表中更新数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="ht">键值表</param>
        /// <param name="filterStr">过滤条件以and开头</param>
        /// <returns>是否更新成功</returns>
        public bool UpdateData(string tableName, Hashtable ht, string filterStr)
        {
            string sql = string.Format("update {0} set ", tableName);
            List<IDbDataParameter> paras = new List<IDbDataParameter>();
            foreach (System.Collections.DictionaryEntry item in ht)
            {
                if (item.Value == null)
                {
                    sql += " " + item.Key.ToString() + "=null,";
                }
                else
                {
                    sql += " " + item.Key.ToString() + "=@" + item.Key.ToString() + ",";
                    paras.Add(new SqlParameter()
                    {
                        ParameterName = item.Key.ToString(),
                        Value = item.Value
                    });
                }
            }
            sql = sql.TrimEnd(new char[] { ',' });
            sql += " where 1=1 ";
            sql += filterStr;
            return ExecuteSql(sql, paras.ToArray()) > 0 ? true : false;
        }

        /// <summary>
        /// 根据键值表中的数据向表中更新数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="dic">键值表</param>
        /// <param name="filterStr">过滤条件以and开头</param>
        /// <returns>是否更新成功</returns>
        public bool UpdateData(string tableName, Dictionary<string, object> dic, string filterStr)
        {
            Hashtable ht = new Hashtable();
            foreach (var i in dic)
            {
                ht.Add(i.Key, i.Value);
            }
            return UpdateData(tableName, ht, filterStr);
        }

        /// <summary>
        /// 根据键值表中的数据向表中更新数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="ht">键值表</param>
        /// <param name="filterStr">过滤条件以and开头</param>
        /// <param name="paraArr">过滤条件中的参数数组</param>
        /// <returns>是否更新成功</returns>
        public bool UpdateData(string tableName, Hashtable ht, string filterStr, IDbDataParameter[] paraArr)
        {
            string sql = string.Format("update {0} set ", tableName);
            List<IDbDataParameter> paras = new List<IDbDataParameter>();
            foreach (System.Collections.DictionaryEntry item in ht)
            {
                if (item.Value == null)
                {
                    sql += " " + item.Key.ToString() + "=null,";
                }
                else
                {
                    sql += " " + item.Key.ToString() + "=@" + item.Key.ToString() + ",";
                    paras.Add(new SqlParameter()
                    {
                        ParameterName = item.Key.ToString(),
                        Value = item.Value
                    });
                }
            }
            sql = sql.TrimEnd(new char[] { ',' });
            sql += " where 1=1 ";
            sql += filterStr;
            foreach (var item in paraArr)
            {
                if (!ContainsDBParameter(paras, item))
                {
                    paras.Add(item);
                }
            }
            return ExecuteSql(sql, paras.ToArray()) > 0 ? true : false;
        }

        /// <summary>
        /// 根据键值表中的数据向表中更新数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="dic">键值表</param>
        /// <param name="filterStr">过滤条件以and开头</param>
        /// <param name="paraArr">过滤条件中的参数数组</param>
        /// <returns>是否更新成功</returns>
        public bool UpdateData(string tableName, Dictionary<string, object> dic, string filterStr, IDbDataParameter[] paraArr)
        {
            Hashtable ht = new Hashtable();
            foreach (var i in dic)
            {
                ht.Add(i.Key, i.Value);
            }
            return UpdateData(tableName, ht, filterStr, paraArr);
        }

        /// <summary>
        /// 向表中更新或添加数据并根据键值对作为关键字更新(关键字默认不参与更新)
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="ht">键值表</param>
        /// <param name="keys">关键字集合</param>
        /// <param name="isKeyAttend">关键字是否参与到更新中</param>
        /// <returns>是否更新成功</returns>
        public bool UpdateData(string tableName, Hashtable ht, List<string> keys, bool isKeyAttend = false)
        {
            string sql = string.Format("update {0} set ", tableName);
            List<IDbDataParameter> paras = new List<IDbDataParameter>();
            foreach (System.Collections.DictionaryEntry item in ht)
            {
                if (keys.Contains(item.Key.ToString()) && !isKeyAttend)
                {
                    continue;
                }
                if (item.Value == null)
                {
                    sql += " " + item.Key.ToString() + "=null,";
                }
                else
                {
                    sql += " " + item.Key.ToString() + "=@" + item.Key.ToString() + ",";
                    IDbDataParameter para = new SqlParameter()
                    {
                        ParameterName = item.Key.ToString(),
                        Value = item.Value
                    };
                    if (!ContainsDBParameter(paras, para))
                    {
                        paras.Add(para);
                    }
                }
            }
            sql = sql.TrimEnd(new char[] { ',' });
            sql += " where 1=1 ";
            foreach (var item in keys)
            {
                sql += " and " + item + "=@" + item;
                paras.Add(new SqlParameter()
                {
                    ParameterName = item,
                    Value = ht[item]
                });
            }
            return ExecuteSql(sql, paras.ToArray()) > 0 ? true : false;
        }

        /// <summary>
        /// 向表中更新数据并根据键值表里面的键值对作为关键字更新(关键字默认不参与更新)
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="dic">键值表</param>
        /// <param name="keys">关键字集合</param>
        /// <param name="isKeyAttend">关键字是否参与到更新中</param>
        /// <returns>是否更新成功</returns>
        public bool UpdateData(string tableName, Dictionary<string, object> dic, List<string> keys, bool isKeyAttend = false)
        {
            Hashtable ht = new Hashtable();
            foreach (var i in dic)
            {
                ht.Add(i.Key, i.Value);
            }
            return UpdateData(tableName, ht, keys, isKeyAttend);
        }

        /// <summary>
        /// 根据键值表中的数据向表中添加或更新数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="ht">键值表</param>
        /// <param name="filterStr">过滤条件以and开头</param>
        /// <returns>是否更新成功</returns>
        public bool UpdateOrAdd(string tableName, Hashtable ht, string filterStr)
        {
            if (GetFirstColumnString(string.Format("select count(1) from {0} where 1=1 {1}", tableName, filterStr)) == "0")
            {
                return AddData(tableName, ht);
            }
            else
            {
                return UpdateData(tableName, ht, filterStr);
            }
        }

        /// <summary>
        /// 根据键值表中的数据向表中添加或更新数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="dic">键值表</param>
        /// <param name="filterStr">过滤条件以and开头</param>
        /// <returns>是否更新成功</returns>
        public bool UpdateOrAdd(string tableName, Dictionary<string, object> dic, string filterStr)
        {
            Hashtable ht = new Hashtable();
            foreach (var i in dic)
            {
                ht.Add(i.Key, i.Value);
            }
            return UpdateOrAdd(tableName, ht, filterStr);
        }

        /// <summary>
        /// 根据键值表中的数据向表中添加或更新数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="ht">键值表</param>
        /// <param name="filterStr">过滤条件以and开头</param>
        /// <param name="paraArr">过滤条件中的参数数组</param>
        /// <returns>是否更新成功</returns>
        public bool UpdateOrAdd(string tableName, Hashtable ht, string filterStr, IDbDataParameter[] paraArr)
        {
            if (GetFirstColumnString(string.Format("select count(1) from {0} where 1=1 {1}", tableName, filterStr), paraArr) == "0")
            {
                return AddData(tableName, ht);
            }
            else
            {
                return UpdateData(tableName, ht, filterStr, paraArr);
            }
        }

        /// <summary>
        /// 根据键值表中的数据向表中添加或更新数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="dic">键值表</param>
        /// <param name="filterStr">过滤条件以and开头</param>
        /// <param name="paraArr">过滤条件中的参数数组</param>
        /// <returns>是否更新成功</returns>
        public bool UpdateOrAdd(string tableName, Dictionary<string, object> dic, string filterStr, IDbDataParameter[] paraArr)
        {
            Hashtable ht = new Hashtable();
            foreach (var i in dic)
            {
                ht.Add(i.Key, i.Value);
            }
            return UpdateOrAdd(tableName, ht, filterStr, paraArr);
        }

        /// <summary>
        /// 向表中添加或更新数据并根据键值对作为关键字更新(关键字默认不参与更新)
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="ht">键值表</param>
        /// <param name="keys">关键字集合</param>
        /// <param name="isKeyAttend">关键字是否参与到更新中</param>
        /// <returns>是否更新成功</returns>
        public bool UpdateOrAdd(string tableName, Hashtable ht, List<string> keys, bool isKeyAttend = false)
        {
            List<IDbDataParameter> paraList = new List<IDbDataParameter>();
            string filter = "";
            keys.ForEach((i) =>
            {
                paraList.Add(CreatePara(i, ht[i]));
                filter += string.Format(" and {0}=" + paraPrefix + i, i);
            });
            if (GetFirstColumnString(string.Format("select count(1) from {0} where 1=1 {1}", tableName, filter), paraList.ToArray()) == "0")
            {
                return AddData(tableName, ht);
            }
            else
            {
                return UpdateData(tableName, ht, keys, isKeyAttend);
            }
        }

        /// <summary>
        /// 向表中添加或更新数据并根据键值对作为关键字更新(关键字默认不参与更新)
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="dic">键值表</param>
        /// <param name="keys">关键字集合</param>
        /// <param name="isKeyAttend">关键字是否参与到更新中</param>
        /// <returns>是否更新成功</returns>
        public bool UpdateOrAdd(string tableName, Dictionary<string, object> dic, List<string> keys, bool isKeyAttend = false)
        {
            Hashtable ht = new Hashtable();
            foreach (var i in dic)
            {
                ht.Add(i.Key, i.Value);
            }
            return UpdateOrAdd(tableName, ht, keys, isKeyAttend);
        }

        /// <summary>
        /// 判断参数集合list中是否包含同名的参数para,如果已存在返回true,否则返回false
        /// </summary>
        /// <param name="list">参数集合</param>
        /// <param name="para">参数模型</param>
        /// <returns>参数集合中是否包含参数模型</returns>
        private bool ContainsDBParameter(List<IDbDataParameter> list, IDbDataParameter para)
        {
            for (int i = 0; i < list.Count; i++)
            {
                if (list[i].ParameterName == para.ParameterName)
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// 删除一行
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="strFilter">过滤条件以and开头</param>
        /// <returns>返回受影响的行数</returns>
        public int DeleteTableRow(string tableName, string strFilter)
        {
            string sql = string.Format("delete from {0} where 1=1 {1}", tableName, strFilter);
            return ExecuteSql(sql);
        }

        /// <summary>
        /// 删除一行
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="strFilter">过滤条件</param>
        /// <param name="paras">过滤条件中的参数集合</param>
        /// <returns>返回受影响的行数</returns>
        public int DeleteTableRow(string tableName, string strFilter, IDbDataParameter[] paras)
        {
            string sql = string.Format("delete from {0} where 1=1 {1}", tableName, strFilter);
            return ExecuteSql(sql, paras.ToArray());
        }

        /// <summary>
        /// 返回查到的第一行第一列的值
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回查到的第一行第一列的值</returns>
        public object GetFirstColumn(string strSql)
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
            object obj = cmd.ExecuteScalar();
            if (!IsTran && !IsKeepConnect)
            {
                conn.Close();
                IsOpen = false;
            }
            return obj;
        }

        /// <summary>
        /// 返回查到的第一行第一列的值
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="paraArr">sql语句参数</param>
        /// <returns>返回查到的第一行第一列的值</returns>
        public object GetFirstColumn(string strSql, IDbDataParameter[] paraArr)
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
            object obj = cmd.ExecuteScalar();
            cmd.Parameters.Clear();
            if (!IsTran && !IsKeepConnect)
            {
                conn.Close();
                IsOpen = false;
            }
            return obj;
        }

        /// <summary>
        /// 返回查到的第一行第一列的字符串值(调用GetFirstColumn,将返回的对象转换成字符串,如果为null就转化为"")
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="isReturnNull">当查询结果为null是是否将null返回,为true则返回null,为false则返回"",默认为false</param>
        /// <returns>返回查到的第一行第一列的值</returns>
        public string GetFirstColumnString(string strSql, bool isReturnNull = false)
        {
            object obj = GetFirstColumn(strSql);
            if (obj == null)
            {
                if (isReturnNull)
                {
                    return null;
                }
                else
                {
                    return "";
                }
            }
            else
            {
                return obj.ToString();
            }
        }

        /// <summary>
        /// 返回查到的第一行第一列的字符串值
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="paraArr">sql语句中的参数数组</param>
        /// <param name="isReturnNull">false:查询结果为null就返回""否则返回null</param>
        /// <returns>返回查到的第一行第一列的值</returns>
        public string GetFirstColumnString(string strSql, IDbDataParameter[] paraArr, bool isReturnNull = false)
        {
            object obj = GetFirstColumn(strSql, paraArr);
            if (obj == null)
            {
                if (isReturnNull)
                {
                    return null;
                }
                else
                {
                    return "";
                }
            }
            else
            {
                return obj.ToString();
            }
        }

        /// <summary>
        /// 返回查到的第一行第一列的字符串值
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="paraArr">sql语句中的参数数组</param>
        /// <param name="isReturnNull">false:查询结果为null就返回""否则返回null</param>
        /// <returns>返回查到的第一行第一列的值</returns>
        public string GetFirstColumnString(string strSql, bool isReturnNull = false, params IDbDataParameter[] paraArr)
        {
            return GetFirstColumnString(strSql, paraArr, isReturnNull);
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

            cmd.CommandTimeout = 10000;
            try
            {
                if (conn.State != ConnectionState.Open)
                {
                    conn.Open();
                }

                IDataReader reader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                return reader;
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
            SqlCommand cmd = new SqlCommand(strSql, (SqlConnection)conn)
            {
                CommandTimeout = 10000
            };
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
        /// 判断指定表中是否有某一列
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="columnName">列名</param>
        /// <returns>返回列是否存在</returns>
        public bool JudgeColumnExist(string tableName, string columnName)
        {
            string sql = string.Format("select count(1) from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='{0}' and COLUMN_NAME='{1}'", tableName, columnName);
            int r = int.Parse(GetFirstColumn(sql).ToString());
            if (r > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// 判断表是否存在
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <returns>返回表是否存在</returns>
        public bool JudgeTableOrViewExist(string tableName)
        {
            string sql = string.Format("select count(1) from INFORMATION_SCHEMA.TABLES where TABLE_NAME='{0}'", tableName);
            int r = int.Parse(GetFirstColumn(sql).ToString());
            if (r > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
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
        /// 重命名指定表
        /// </summary>
        /// <param name="oldTableName">旧表名</param>
        /// <param name="newTableName">新表名</param>
        public void RenameTable(string oldTableName, string newTableName)
        {
            string sql = "EXEC   sp_rename   '" + oldTableName + "',   '" + newTableName + "'";
            ExecuteSql(sql);
        }

        /// <summary>
        /// 删除指定表
        /// </summary>
        /// <param name="tableName">要删除的表</param>
        /// <returns></returns>
        public void DropTable(string tableName)
        {
            string sql = "drop  table   " + tableName + "";
            ExecuteSql(sql);
        }

        /// <summary>
        /// 保存表说明,如果不存在旧的说明信息就创建否则就覆盖
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="desc">说明信息</param>
        /// <returns></returns>
        public void SaveTableDesc(string tableName, string desc)
        {
            string sql = string.Format(@"SELECT count(1)
FROM fn_listextendedproperty ('MS_Description', 'schema', 'dbo', 'table', '{0}',null,null);", tableName);
            if (GetFirstColumnString(sql) == "1")
            {
                sql = string.Format(@"EXEC sp_dropextendedproperty @name=N'MS_Description', @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'{0}', @level2type=null,@level2name=null", tableName);
                ExecuteSql(sql);
            }
            sql = string.Format(@"EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'{0}' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'{1}', @level2type=null,@level2name=null", desc, tableName);
            ExecuteSql(sql);
        }

        /// <summary>
        /// 重命名列名
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="oldColumnName">旧的列名</param>
        /// <param name="newColumnName">新的列名</param>
        /// <returns></returns>
        public void RenameColumn(string tableName, string oldColumnName, string newColumnName)
        {
            string sql = string.Format("sp_rename '{0}.{1}','{2}','column' ", tableName, oldColumnName, newColumnName);
            ExecuteSql(sql);
        }

        /// <summary>
        /// 删除指定表的指定列
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="columnName">要删除的列名</param>
        /// <returns></returns>
        public void DropColumn(string tableName, string columnName)
        {
            string sql = string.Format(" ALTER TABLE {0} DROP COLUMN {1}", tableName, columnName);
            ExecuteSql(sql);
        }

        /// <summary>
        /// 保存指定表的指定列的说明信息
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="columnName">列名</param>
        /// <param name="desc">说明信息</param>
        /// <returns></returns>
        public void SaveColumnDesc(string tableName, string columnName, string desc)
        {
            string sql = string.Format(@"SELECT count(1)
FROM fn_listextendedproperty ('MS_Description', 'schema', 'dbo', 'table', '{0}','column','{1}');", tableName, columnName);
            if (GetFirstColumnString(sql) == "1")
            {
                sql = string.Format(@"--删除列说明
  EXEC sp_dropextendedproperty @name=N'MS_Description', @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'{0}', @level2type='column',@level2name='{1}'", tableName, columnName);
                ExecuteSql(sql);
            }
            sql = string.Format(@"EXECUTE sp_addextendedproperty N'MS_Description', '{0}', N'schema', N'dbo', N'table', N'{1}', N'column', N'{2}'", desc, tableName, columnName);
            ExecuteSql(sql);
        }

        /// <summary>
        /// 改变指定表的指定列类型
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="columnName">列名</param>
        /// <param name="columnType">列类型</param>
        /// <param name="isForce">是否暴力修改列类型,暴力修改:在正常修改不成功的情况下会删掉这个列并重建这个列,数据将会丢失</param>
        /// <returns></returns>
        public Result AlterColumnType(string tableName, string columnName, string columnType, bool isForce)
        {
            string sql = string.Format("alter table {0} alter column {1} {2}", tableName, columnName, columnType);
            try
            {
                ExecuteSql(sql);
                return new Result() { Success = true };
            }
            catch (Exception ex)
            {
                if (isForce)
                {
                    sql = string.Format("ALTER TABLE {0} DROP COLUMN {1}", tableName, columnName);
                    ExecuteSql(sql);
                    sql = string.Format("ALTER TABLE {0} ADD {1} {2}", tableName, columnName, columnType);
                    ExecuteSql(sql);
                    return new Result() { Success = true };
                }
                else
                {
                    return new Result() { Success = false, Data = ex.ToString() };
                }
            }
        }

        /// <summary>
        /// 修改指定表的指定列是否可以为空
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="columnName">列名</param>
        /// <param name="columnType">列类型</param>
        /// <param name="canNull">是否可空</param>
        /// <returns></returns>
        public void AlterColumnNullAble(string tableName, string columnName, string columnType, bool canNull)
        {
            string sql = string.Format("alter table {0} alter column {1} {2} {3}", tableName, columnName, columnType, canNull ? "null" : "not null");
            ExecuteSql(sql);
        }

        /// <summary>
        /// 给指定表增加自增列
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="columnName">列名</param>
        /// <param name="columnType">列类型</param>
        /// <param name="start">种子</param>
        /// <param name="end">增量</param>
        /// <returns></returns>
        public void AddIdentityColumn(string tableName, string columnName, string columnType, string start, string end)
        {
            string sql = string.Format(" alter table {0} add {1} int identity({2},{3}) ", tableName, columnName, columnType, start, end);
            ExecuteSql(sql);
        }

        /// <summary>
        /// 给指定列修改默认值
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="columnName">列名</param>
        /// <param name="def">默认值</param>
        /// <returns></returns>
        public void SaveColumnDefault(string tableName, string columnName, string def)
        {
            string sql = string.Format(@"SELECT name FROM sysobjects WHERE id = ( SELECT syscolumns.cdefault FROM sysobjects
    INNER JOIN syscolumns ON sysobjects.Id=syscolumns.Id
    WHERE sysobjects.name=N'{0}' AND syscolumns.name=N'{1}' )", tableName, columnName);
            string defname = GetFirstColumnString(sql);
            if (string.IsNullOrEmpty(def))
            {
                if (defname != "")
                {
                    sql = string.Format(" ALTER TABLE {0} DROP CONSTRAINT {1}", tableName, defname);
                    ExecuteSql(sql);
                }
            }
            else
            {
                if (defname != "")
                {
                    sql = string.Format(" ALTER TABLE {0} DROP CONSTRAINT {1}", tableName, defname);
                    ExecuteSql(sql);
                }
                sql = string.Format(" ALTER TABLE {0} ADD CONSTRAINT DF_gene_{0}_{1} DEFAULT ('{2}') FOR {1}", tableName, columnName, def);
                ExecuteSql(sql);
            }
        }

        /// <summary>
        /// 删除指定表指定列的默认值
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="columnName">列名</param>
        /// <returns></returns>
        public void DropColumnDefault(string tableName, string columnName)
        {
            string sql = string.Format(@"SELECT name FROM sysobjects WHERE id = ( SELECT syscolumns.cdefault FROM sysobjects
    INNER JOIN syscolumns ON sysobjects.Id=syscolumns.Id
    WHERE sysobjects.name=N'{0}' AND syscolumns.name=N'{1}' )", tableName, columnName);
            string defname = GetFirstColumnString(sql);
            if (defname != "")
            {
                sql = string.Format(" ALTER TABLE {0} DROP CONSTRAINT {1}", tableName, defname);
                ExecuteSql(sql);
            }
        }

        /// <summary>
        /// 创建新表
        /// </summary>
        /// <param name="tableStruct">表结构说明</param>
        /// <returns></returns>
        public void CreateTable(TableStruct tableStruct)
        {
            string sql = string.Format(@" create table [{0}] (
", tableStruct.Name);
            string sqlPri = @"
ALTER TABLE [{0}] ADD CONSTRAINT PK_gene_{0}_{1} PRIMARY KEY({2})";
            string priname = "";
            string prikey = "";
            string sqldesc = "";
            if (!string.IsNullOrWhiteSpace(tableStruct.Desc))
            {
                sqldesc += string.Format(@"
 EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'{0}' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'{1}', @level2type=null,@level2name=null", tableStruct.Desc, tableStruct.Name);
            }

            tableStruct.Columns.ForEach(i =>
            {
                string ideSql = "";
                string nullSql = "";
                string defSql = "";
                string uniSql = "";
                if (i.IsIdentity)
                {
                    ideSql = "identity(" + i.Start + "," + i.Incre + ")";
                }
                if (i.IsUnique)
                {
                    uniSql = "unique";
                }
                if (!i.IsNullable)
                {
                    nullSql = "not null";
                }
                if (!string.IsNullOrWhiteSpace(i.Default))
                {
                    defSql = " default '" + i.Default + "'";
                }
                if (i.IsPrimaryKey)
                {
                    priname += "_" + i.Name;
                    prikey += "," + i.Name;
                }

                sql += string.Format(@" [{0}] {1} {2} {3} {4} {5},
", i.Name, i.Type, nullSql, defSql, ideSql, uniSql);
                if (i.Desc != "" && i.Desc != null)
                {
                    sqldesc += string.Format(@"
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'{0}' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'{1}', @level2type=N'COLUMN',@level2name=N'{2}'
", i.Desc, tableStruct.Name, i.Name);
                }
            });
            priname = priname.Trim('_');
            prikey = prikey.Trim(',');
            if (prikey.Contains(","))
            {
                string[] arr = prikey.Split(',');
                prikey = "";
                for (int i = 0; i < arr.Length; i++)
                {
                    prikey += "[" + arr[i] + "],";
                }
                prikey = prikey.Trim(',');
            }
            sqlPri = string.Format(sqlPri, tableStruct.Name, priname, prikey);
            if (prikey == "")
            {
                sqlPri = "";
            }
            sql += @"
)
";
            ExecuteSql(sql);
            ExecuteSql(sqlPri);
            ExecuteSql(sqldesc);
        }

        /// <summary>
        /// 给指定表添加一列
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="column">列名</param>
        /// <returns></returns>
        public void AddColumn(string tableName, Column column)
        {
            string sql = " alter table " + tableName + " add " + column.Name + " " + column.Type;
            string sqlDesc = "";
            if (!string.IsNullOrWhiteSpace(column.Desc))
            {
                sqlDesc = string.Format("EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'{0}' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'{1}', @level2type=N'COLUMN',@level2name=N'{2}'", column.Desc, tableName, column.Name);
            }
            if (column.IsNullable)
            {
                sql += " null";
            }
            else
            {
                sql += " not null";
            }
            if (column.IsIdentity)
            {
                sql += " identity(" + column.Start + "," + column.Incre + ")";
            }
            if (column.IsUnique)
            {
                sql += " unique";
            }
            if (!string.IsNullOrEmpty(column.Default))
            {
                if (column.Type.Contains("char") || column.Type.Contains("date") || column.Type.Contains("text"))
                {
                    sql += " default '" + column.Default + "'";
                }
                else
                {
                    sql += " default " + column.Default;
                }
            }
            ExecuteSql(sql);
            if (sqlDesc != "")
            {
                ExecuteSql(sqlDesc);
            }
        }

        /// <summary>
        /// 设置指定列是否是唯一的
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="columnName">列名</param>
        /// <param name="canUnique">是否是唯一的</param>
        public void SaveColumnUnique(string tableName, string columnName, bool canUnique)
        {
            string sql = @"
SELECT idx.name
  FROM sys.indexes idx
  JOIN sys.index_columns idxCol ON (idx.object_id = idxCol.object_id AND
                                   idx.index_id = idxCol.index_id AND
                                   idx.is_unique_constraint = 1)
  JOIN sys.tables tab ON (idx.object_id = tab.object_id)
  JOIN sys.columns col ON (idx.object_id = col.object_id AND
                          idxCol.column_id = col.column_id)
 WHERE tab.name = 'test'
   and col.name = 'gh'";
            string constraintName = GetFirstColumnString(sql);
            if (!canUnique && constraintName != "")
            {
                //删除唯一约束
                ExecuteSql(string.Format("ALTER TABLE {0} DROP CONSTRAINT {1}", tableName, constraintName));
            }
            if (canUnique && constraintName == "")
            {
                //增加唯一约束
                ExecuteSql(string.Format("ALTER TABLE {0} ADD CONSTRAINT UQ_gene_{0}_{1} UNIQUE ({1})", tableName, columnName));
            }
        }

        /// <summary>
        /// 准备数据导出的存储过程,这将重建usp_CreateInsertScript
        /// </summary>
        //     public void PreExportDataProc()
        //     {
        ////         ExecuteSql(@"if exists (select 1
        ////         from  sys.procedures
        ////        where  name = 'spGenInsertSQL')
        ////drop proc spGenInsertSQL");
        //         ExecuteSql(sqlExportDataProc);
        //     }

        /// <summary>
        /// 根据表结构对象生成建表语句
        /// </summary>
        /// <param name="tableStruct"></param>
        /// <returns></returns>
        public string CreateTableSql(TableStruct tableStruct)
        {
            string sql = string.Format(@"create table [{0}] (
", tableStruct.Name);
            string sqlPri = @"
ALTER TABLE [{0}] ADD CONSTRAINT PK_gene_{0}_{1} PRIMARY KEY({2})";
            string priname = "";
            string prikey = "";
            string sqldesc = "";
            if (!string.IsNullOrWhiteSpace(tableStruct.Desc))
            {
                sqldesc += string.Format(@"
 EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'{0}' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'{1}', @level2type=null,@level2name=null", tableStruct.Desc, tableStruct.Name);
            }

            tableStruct.Columns.ForEach(i =>
            {
                string ideSql = "";
                string nullSql = "";
                string defSql = "";
                string uniSql = "";
                ideSql = i.FinalIdentity;
                if (i.IsUnique)
                {
                    uniSql = "unique";
                }
                if (!i.IsNullable)
                {
                    nullSql = "not null";
                }
                if (!string.IsNullOrWhiteSpace(i.Default))
                {
                    defSql = " default '" + i.Default + "'";
                }
                if (i.IsPrimaryKey)
                {
                    priname += "_" + i.Name;
                    prikey += "," + i.Name;
                }

                sql += string.Format(@"    [{0}] {1} {2} {3} {4} {5},
", i.Name, i.FinalType, nullSql, defSql, ideSql, uniSql);
                if (i.Desc != "" && i.Desc != null)
                {
                    sqldesc += string.Format(@"
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'{0}' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'{1}', @level2type=N'COLUMN',@level2name=N'{2}'
", i.Desc, tableStruct.Name, i.Name);
                }
            });
            sql = sql.Trim('\n', '\r', ',');
            priname = priname.Trim('_');
            prikey = prikey.Trim(',');
            if (prikey.Contains(","))
            {
                string[] arr = prikey.Split(',');
                prikey = "";
                for (int i = 0; i < arr.Length; i++)
                {
                    prikey += "[" + arr[i] + "],";
                }
                prikey = prikey.Trim(',');
            }
            sqlPri = string.Format(sqlPri, tableStruct.Name, priname, prikey);
            if (prikey == "")
            {
                sqlPri = "";
            }
            sql += @"
)
";
            string res = string.Format("{0}\r\n {1}\r\n {2}", sql, sqlPri, sqldesc);

            //构建约束语句
            //六类约束:主键、非空、默认、唯一、检查、外键,这里只对后两个约束生成语句
            string sqlConstraint = "";
            tableStruct.Constraints.ForEach(i =>
            {
                if (i.Type == "检查约束")
                {
                    sqlConstraint += "\r\n--************检查约束<" + i.Name + ">*****************\r\n";
                    string tmp = string.Format("ALTER TABLE {0} ADD CONSTRAINT {1} CHECK({2})", tableStruct.Name, i.Name, i.Remark.Trim('(', ')'));
                    sqlConstraint += tmp;
                    sqlConstraint += "\r\n--************检查约束</" + i.Name + ">*****************\r\n";
                }
                else if (i.Type == "外键约束")
                {
                    sqlConstraint += "\r\n--************外键约束<" + i.Name + ">*****************\r\n";
                    string tmp = string.Format("ALTER TABLE {0} ADD CONSTRAINT {1} FOREIGN KEY({2}) {3} ON DELETE {4} ON UPDATE {5}", tableStruct.Name, i.Name, i.Keys, i.RefStr, i.DelType, i.UpdateType);
                    sqlConstraint += tmp;
                    sqlConstraint += "\r\n--************外键约束</" + i.Name + ">*****************\r\n";
                }
            });
            //构建触发器语句
            string sqlTrigger = "";
            tableStruct.Triggers.ForEach(i =>
            {
                sqlTrigger += "\r\n--************触发器<" + i.Name + ">*****************\r\ngo\r\n";
                DataTable dt3 = GetDataTable("sp_helptext '" + i.Name + "'");
                for (int k = 0; k < dt3.Rows.Count; k++)
                {
                    sqlTrigger += dt3.Rows[k][0].ToString();
                }
                sqlTrigger += "go\r\n--************触发器</" + i.Name + ">*****************\r\ngo\r\n";
            });
            //构建索引语句
            string sqlIndex = "";
            tableStruct.Indexs.ForEach(i =>
            {
                sqlIndex += "\r\n--************索引<" + i.Name + ">*****************\r\n";
                if (i.Desc.Contains("unique"))
                {
                    sqlIndex += string.Format("CREATE UNIQUE NONCLUSTERED INDEX {0} ON test({1})", i.Name, i.Keys);
                }
                else
                {
                    sqlIndex += string.Format("CREATE NONCLUSTERED INDEX {0} ON test({1})", i.Name, i.Keys);
                }

                sqlIndex += "\r\n--************索引</" + i.Name + ">*****************\r\n";
            });
            res += string.Format("\r\n{0}\r\n{1}\r\n{2}", sqlConstraint, sqlTrigger, sqlIndex);
            return res;
        }

        /// <summary>
        /// 根据视图名称生成视图建立语句
        /// </summary>
        /// <param name="viewName">视图名称</param>
        /// <returns></returns>
        public string CreateViewSql(string viewName)
        {
            if (!string.IsNullOrWhiteSpace(viewName) && viewName.StartsWith("["))
            {
                viewName = "[" + viewName + "]";
            }
            StringBuilder sb = new StringBuilder();

            DataTable dt = GetDataTable("sp_helptext " + viewName);
            for (int ii = 0; ii < dt.Rows.Count; ii++)
            {
                sb.Append(dt.Rows[ii][0].ToString());
            }
            return sb.ToString();
        }

        /// <summary>
        /// 根据存储过程名字生成存储过程的创建语句
        /// </summary>
        /// <param name="procName">存储过程名字</param>
        /// <returns></returns>
        public string CreateProcSql(string procName)
        {
            string sql = @"select ROUTINE_DEFINITION
from INFORMATION_SCHEMA.ROUTINES
where ROUTINE_TYPE='PROCEDURE' and ROUTINE_NAME='" + procName + "'\r\n";
            return GetFirstColumnString(sql);
        }

        /// <summary>
        /// 根据函数名生成函数的创建语句
        /// </summary>
        /// <param name="funcName">函数名称</param>
        /// <returns></returns>
        public string CreateFuncSql(string funcName)
        {
            string sql = @"sp_helptext '" + funcName + "'";
            DataTable dt = GetDataTable(sql);
            StringBuilder sb = new StringBuilder();
            if (dt.Rows.Count > 0)
            {
                for (int ii = 0; ii < dt.Rows.Count; ii++)
                {
                    sb.Append(dt.Rows[ii][0].ToString());
                }
            }
            return sb.ToString();
        }

        /// <summary>
        /// 根据表名称和过滤条件生成表数据的insert语句
        /// </summary>
        /// <param name="tblName">表结构</param>
        /// <param name="Count">生成的insert语句的个数</param>
        /// <param name="filter">过滤条件</param>
        /// <returns></returns>
        public string GeneInsertSql(string tblName, int Count, string filter = "1=1")
        {
            DataTable dt = GetDataTable("exec spGenInsertSQL '" + tblName + "','" + Count + "'" + "','" + filter + "'");
            StringBuilder sb = new StringBuilder();
            sb.AppendLine();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                sb.AppendLine(dt.Rows[i][0].ToString());
            }
            sb.AppendLine();
            return sb.ToString();
        }

        #region string sqlExportDataProc 表数据导出insert语句的存储过程语句

        //        readonly string sqlExportDataProc = @"
        //IF OBJECT_ID('spGenInsertSQL','P') IS NOT NULL
        //DROP PROC spGenInsertSQL
        //GO
        //CREATE   proc spGenInsertSQL (@tablename varchar(256),@number BIGINT,@whereClause NVARCHAR(MAX))
        //as
        //begin
        //declare @sql varchar(8000)
        //declare @sqlValues varchar(8000)
        //set @sql =' ('
        //set @sqlValues = 'values (''+'
        //select @sqlValues = @sqlValues + cols + ' + '','' + ' ,@sql = @sql + '[' + name + '],'
        //  from
        //      (select case
        //                when xtype in (48,52,56,59,60,62,104,106,108,122,127)

        //                     then 'case when '+ name +' is null then ''NULL'' else ' + 'cast('+ name + ' as varchar)'+' end'

        //                when xtype in (58,61,40,41,42)

        //                     then 'case when '+ name +' is null then ''NULL'' else '+''''''''' + ' + 'cast('+ name +' as varchar)'+ '+'''''''''+' end'

        //               when xtype in (167)

        //                     then 'case when '+ name +' is null then ''NULL'' else '+''''''''' + ' + 'replace('+ name+','''''''','''''''''''')' + '+'''''''''+' end'

        //                when xtype in (231)

        //                     then 'case when '+ name +' is null then ''NULL'' else '+'''N'''''' + ' + 'replace('+ name+','''''''','''''''''''')' + '+'''''''''+' end'

        //                when xtype in (175)

        //                     then 'case when '+ name +' is null then ''NULL'' else '+''''''''' + ' + 'cast(replace('+ name+','''''''','''''''''''') as Char(' + cast(length as varchar)  + '))+'''''''''+' end'

        //                when xtype in (239)

        //                     then 'case when '+ name +' is null then ''NULL'' else '+'''N'''''' + ' + 'cast(replace('+ name+','''''''','''''''''''') as Char(' + cast(length as varchar)  + '))+'''''''''+' end'

        //                else '''NULL'''

        //              end as Cols,name

        //         from syscolumns

        //        where id = object_id(@tablename)

        //      ) T
        //IF (@number!=0 AND @number IS NOT NULL)
        //BEGIN
        //set @sql ='select top '+ CAST(@number AS VARCHAR(6000))+' ''INSERT INTO ['+ @tablename + ']' + left(@sql,len(@sql)-1)+') ' + left(@sqlValues,len(@sqlValues)-4) + ')'' from '+@tablename
        //print @sql
        //END
        //ELSE
        //BEGIN
        //set @sql ='select ''INSERT INTO ['+ @tablename + ']' + left(@sql,len(@sql)-1)+') ' + left(@sqlValues,len(@sqlValues)-4) + ')'' from '+@tablename
        //print @sql
        //END

        //PRINT @whereClause
        //IF ( @whereClause IS NOT NULL  AND @whereClause <> '')
        //BEGIN
        //set @sql =@sql+' where '+@whereClause
        //print @sql
        //END

        //exec (@sql)
        //end
        //GO

        //";

        #endregion string sqlExportDataProc 表数据导出insert语句的存储过程语句

        /// <summary>
        /// 根据当前的数据库类型和连接字符串创建一个新的数据库操作对象
        /// </summary>
        /// <returns></returns>
        public IDbAccess CreateNewIDB()
        {
            return IDBFactory.CreateIDB(ConnectionString, DataBaseType);
        }

        //#region 存储过程操作
        //#endregion

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

            var newcolumns= columns1.Intersect(columns2);

            ////仅仅导入列名一致的表
            //foreach (var cl in columns)
            //{
            //    if (cols.Contains(cl))
            //    {
            //        bulkCopy.ColumnMappings.Add(cl, cl);
            //    }
            //}

            foreach (var cl in newcolumns)
            {              
               bulkCopy.ColumnMappings.Add(cl, cl);               
            }


            try
            {
                //reader.Read();
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

            bulkCopy.BulkCopyTimeout = 1000;


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