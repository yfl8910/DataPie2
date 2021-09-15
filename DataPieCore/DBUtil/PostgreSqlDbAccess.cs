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
            DateTime dt;
            if (DateTime.TryParse(minDate, out dt) && DateTime.TryParse(maxDate, out dt))
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
                insertTableValues += ":" + item.Key.ToString() + ",";
                paras.Add(new NpgsqlParameter()
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
                    sql += " " + item.Key.ToString() + "=:" + item.Key.ToString() + ",";
                    paras.Add(new NpgsqlParameter()
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
                    sql += " " + item.Key.ToString() + "=:" + item.Key.ToString() + ",";
                    paras.Add(new NpgsqlParameter()
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
                    sql += " " + item.Key.ToString() + "=:" + item.Key.ToString() + ",";
                    IDbDataParameter para = new NpgsqlParameter()
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
                sql += " and " + item + "=:" + item;
                paras.Add(new NpgsqlParameter()
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
        /// 根据键值表ht中的数据向表中添加或更新数据
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
        /// 向表中添加或更新数据并根据ht里面的键值对作为关键字更新(关键字默认不参与更新)
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
            object obj = cmd.ExecuteScalar();
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
        /// 获得所有表,注意返回的集合中的表模型中只有表名
        /// </summary>
        /// <returns></returns>

        /// <summary>
        /// 重命名指定表
        /// </summary>
        /// <param name="oldTableName">旧表名</param>
        /// <param name="newTableName">新表名</param>
        public void RenameTable(string oldTableName, string newTableName)
        {
            oldTableName ??= "";
            oldTableName = WrapName(oldTableName);
            newTableName = WrapName(newTableName);
            string sql = string.Format("alter table {0} rename to {1}", oldTableName, newTableName);
            ExecuteSql(sql);
        }

        /// <summary>
        /// 删除指定表
        /// </summary>
        /// <param name="tableName">要删除的表</param>
        /// <returns></returns>
        public void DropTable(string tableName)
        {
            tableName ??= "";
            string wrapTableName = "";
            if (!string.IsNullOrWhiteSpace(tableName) || !tableName.StartsWith("\""))
            {
                wrapTableName = "\"" + tableName + "\"";
            }
            string sql = "drop  table   " + wrapTableName + "";
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
            tableName ??= "";
            tableName = WrapName(tableName);
            desc = (desc ?? "").Replace("\"", "\"\"");
            string sql = string.Format(@"COMMENT ON TABLE {0}
  IS '{1}'", tableName, desc);
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
            string sql = string.Format("alter table {0} rename {1} to {2}", WrapName(tableName), WrapName(oldColumnName), WrapName(newColumnName));
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
            string sql = string.Format(" ALTER TABLE {0} DROP COLUMN {1}", WrapName(tableName), WrapName(columnName));
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
            string sql = string.Format("COMMENT ON COLUMN public.\"{0}\".\"{1}\" IS '{2}';", tableName, columnName, desc);
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
            string sql = string.Format("alter table {0} alter column {1} type {2} using {1}::{2}", WrapName(tableName), WrapName(columnName), columnType);
            try
            {
                ExecuteSql(sql);
                return new Result() { Success = true };
            }
            catch (Exception ex)
            {
                if (isForce)
                {
                    sql = string.Format("ALTER TABLE {0} DROP COLUMN {1}", WrapName(tableName), WrapName(columnName));
                    ExecuteSql(sql);
                    sql = string.Format("ALTER TABLE {0} ADD {1} {2}", WrapName(tableName), WrapName(columnName), columnType);
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
        /// <param name="def">默认值</param>
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
            string sql = string.Format(" alter table {0} add {1} {2} ", WrapName(tableName), WrapName(column.Name), column.Type);
            if (!column.IsNullable)
            {
                sql += " not null";
            }
            if (column.IsUnique)
            {
                sql += " unique";
            }
            if (!string.IsNullOrWhiteSpace(column.Default))
            {
                sql += " default '" + column.Default.Replace("\"", "\"\"") + "'";
            }
            string sqlDesc = "";
            if (!string.IsNullOrWhiteSpace(column.Desc))
            {
                sqlDesc = string.Format("COMMENT ON COLUMN public.{0}.{1} IS '{2}';", WrapName(tableName), column.Name, column.Desc);
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
        //         ExecuteSql(@"if exists (select 1
        //         from  sys.procedures
        //        where  name = 'usp_CreateInsertScript')
        //drop proc usp_CreateInsertScript");
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
            string sql = string.Format("select definition from pg_views where viewname='{0}'", viewName);
            return GetFirstColumnString(sql);
        }

        /// <summary>
        /// 根据存储过程名字生成存储过程的创建语句
        /// </summary>
        /// <param name="procName">存储过程名字</param>
        /// <returns></returns>
        public string CreateProcSql(string procName)
        {
            string sql = string.Format(@"select  prosrc  from pg_proc  where proname = '{0}';", procName);
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
        public string GeneInsertSql(string tblName, ref int Count, string filter = "1=1")
        {
            DataTable dt = GetDataTable("exec usp_CreateInsertScript '" + tblName + "','" + filter + "'");
            StringBuilder sb = new StringBuilder();
            sb.AppendLine();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                sb.AppendLine(dt.Rows[i][0].ToString());
            }
            sb.AppendLine();
            Count = dt.Rows.Count;
            return sb.ToString();
        }

        /// <summary>
        /// 包装对象名称,如果非全部小写,自动添加双引号
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public string WrapName(string tableName)
        {
            tableName ??= "";
            if (tableName.ToLower() != tableName && !tableName.Contains('\"'))
            {
                tableName = "\"" + tableName + "\"";
            }
            return tableName;
        }

        #region string sqlExportDataProc 表数据导出insert语句的存储过程语句

        //        string sqlExportDataProc = @"
        //-- =============================================
        //-- Author: 胡庆杰
        //-- Create date: 2017-1-10
        //-- Description: 将表数据生成Insert脚本
        //-- Demo : exec usp_CreateInsertScript 'SYSUSER','1=1'
        //-- =============================================
        //create proc [dbo].usp_CreateInsertScript (@tablename varchar(400),@con nvarchar(1000))
        //as
        //begin
        //set nocount on
        //declare @sqlstr varchar(max)
        //declare @sqlstr1 varchar(max)
        //declare @sqlstr2 varchar(max)
        //select @sqlstr='select ''insert '+@tablename
        //select @sqlstr1=''
        //select @sqlstr2='('
        //select @sqlstr1='values (''+'
        //select @sqlstr1=@sqlstr1+col+'+'',''+' ,@sqlstr2=@sqlstr2+name +',' from (select case
        //when a.xtype =173 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar('+convert(varchar(4),a.length*2+2)+'),'+a.name +')'+' end'
        //when a.xtype =104 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar(1),'+a.name +')'+' end'
        //when a.xtype =175 then 'case when '+a.name+' is null then ''NULL'' else '+'''''''''+'+'replace('+a.name+','''''''','''''''''''')' + '+'''''''''+' end'
        //when a.xtype =61 then 'case when '+a.name+' is null then ''NULL'' else '+'''''''''+'+'convert(varchar(23),'+a.name +',121)'+ '+'''''''''+' end'
        //when a.xtype =106 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar('+convert(varchar(4),a.xprec+2)+'),'+a.name +')'+' end'
        //when a.xtype =62 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar(23),'+a.name +',2)'+' end'
        //when a.xtype =56 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar(11),'+a.name +')'+' end'
        //when a.xtype =60 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar(22),'+a.name +')'+' end'
        //when a.xtype =239 then 'case when '+a.name+' is null then ''NULL'' else '+'''''''''+'+'replace('+a.name+','''''''','''''''''''')' + '+'''''''''+' end'
        //when a.xtype =108 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar('+convert(varchar(4),a.xprec+2)+'),'+a.name +')'+' end'
        //when a.xtype =231 then 'case when '+a.name+' is null then ''NULL'' else '+'''''''''+'+'replace('+a.name+','''''''','''''''''''')' + '+'''''''''+' end'
        //when a.xtype =59 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar(23),'+a.name +',2)'+' end'
        //when a.xtype =58 then 'case when '+a.name+' is null then ''NULL'' else '+'''''''''+'+'convert(varchar(23),'+a.name +',121)'+ '+'''''''''+' end'
        //when a.xtype =52 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar(12),'+a.name +')'+' end'
        //when a.xtype =122 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar(22),'+a.name +')'+' end'
        //when a.xtype =127 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar(6),'+a.name +')'+' end'
        //when a.xtype =48 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar(6),'+a.name +')'+' end'
        //when a.xtype =165 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar('+convert(varchar(4),a.length*2+2)+'),'+a.name +')'+' end'
        //when a.xtype =167 then 'case when '+a.name+' is null then ''NULL'' else '+'''''''''+'+'replace('+a.name+','''''''','''''''''''')' + '+'''''''''+' end'
        //else '''NULL'''
        //end as col,a.colid,a.name
        //from syscolumns a where a.id = object_id(@tablename)
        //--
        //and a.name not in(SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.columns c where c.TABLE_NAME=@tablename and COLUMNPROPERTY(
        //      OBJECT_ID(@tablename),COLUMN_NAME,'IsIdentity')=1)
        //--
        //and a.xtype <>189 and a.xtype <>34 and a.xtype <>35 and a.xtype <>36
        //)t order by colid
        //select @sqlstr=@sqlstr+left(@sqlstr2,len(@sqlstr2)-1)+') '+left(@sqlstr1,len(@sqlstr1)-3)+')'' from '+@tablename + ' where 1=1 and ' + isnull(@con,'')
        //print @sqlstr
        //exec( @sqlstr)
        //set nocount off
        //end

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