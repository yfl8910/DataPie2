using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace DBUtil
{
    public partial class OracleDbAccess : IDbAccess
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
        public string paraPrefix { get { return ":"; } }

        /// <summary>
        /// 创建参数
        /// </summary>
        /// <returns></returns>
        public IDbDataParameter CreatePara()
        {
            return new OracleParameter();
        }

        /// <summary>
        /// 创建具有名称和值的参数
        /// </summary>
        /// <returns>针对当前数据库类型的参数对象</returns>
        public IDbDataParameter CreatePara(string name, object value)
        {
            return new OracleParameter(name, value);
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
                    res += " and " + dateColumn + ">=to_date('" + minDate + "','yyyy-MM-dd HH24:mi:ss')";
                }
                else
                {
                    res += " and " + dateColumn + ">to_date('" + minDate + "','yyyy-MM-dd HH24:mi:ss')";
                }
                if (isMaxInclude)
                {
                    res += " and " + dateColumn + "<=to_date('" + maxDate + "','yyyy-MM-dd HH24:mi:ss')";
                }
                else
                {
                    res += " and " + dateColumn + "<to_date('" + maxDate + "','yyyy-MM-dd HH24:mi:ss')";
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
                OracleCommand cmd = new OracleCommand(strSql, (OracleConnection)conn);
                if (IsTran)
                {
                    cmd.Transaction = (OracleTransaction)tran;
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
                OracleCommand cmd = new OracleCommand();
                cmd.Connection = (OracleConnection)conn;
                if (IsTran)
                {
                    cmd.Transaction = (OracleTransaction)tran;
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
                OracleCommand cmd = new OracleCommand(strSql, (OracleConnection)conn);
                if (IsTran)
                {
                    cmd.Transaction = (OracleTransaction)tran;
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
            DataTable dt = GetDataTable(string.Format(" select DATA_TYPE,COLUMN_NAME from user_tab_cols where table_name='{0}'", tableName.ToUpper()));
            Hashtable ht_pre = new Hashtable();
            if (dt.Rows.Count > 0)
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    if (dt.Rows[i]["DATA_TYPE"].ToString().ToUpper() == "DATE")
                    {
                        ht_pre.Add(dt.Rows[i]["COLUMN_NAME"].ToString(), dt.Rows[i]["DATA_TYPE"].ToString());
                    }
                }
            }
            string insertTableOption = "";
            string insertTableValues = "";
            List<IDbDataParameter> paras = new List<IDbDataParameter>();
            foreach (System.Collections.DictionaryEntry item in ht)
            {
                insertTableOption += " " + item.Key.ToString() + ",";
                if (ht_pre.Contains(item.Key.ToString()))
                {
                    insertTableValues += "to_date(:" + item.Key.ToString() + ",'yyyy-MM-dd HH24:mi:ss'),";
                }
                else
                {
                    insertTableValues += ":" + item.Key.ToString() + ",";
                }
                paras.Add(new OracleParameter()
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
            DataTable dt = GetDataTable(string.Format(" select DATA_TYPE,COLUMN_NAME from user_tab_cols where table_name='{0}'", tableName.ToUpper()));
            Hashtable ht_pre = new Hashtable();
            if (dt.Rows.Count > 0)
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    if (dt.Rows[i]["DATA_TYPE"].ToString().ToUpper() == "DATE")
                    {
                        ht_pre.Add(dt.Rows[i]["COLUMN_NAME"].ToString(), dt.Rows[i]["DATA_TYPE"].ToString());
                    }
                }
            }

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
                    if (ht_pre.Contains(item.Key.ToString()))
                    {
                        sql += " " + item.Key.ToString() + "=to_date(:" + item.Key.ToString() + ",'yyyy-MM-dd HH24:mi:ss'),";
                    }
                    else
                    {
                        sql += " " + item.Key.ToString() + "=:" + item.Key.ToString() + ",";
                    }
                    paras.Add(new OracleParameter()
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
            DataTable dt = GetDataTable(string.Format(" select DATA_TYPE,COLUMN_NAME from user_tab_cols where table_name='{0}'", tableName.ToUpper()));
            Hashtable ht_pre = new Hashtable();
            if (dt.Rows.Count > 0)
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    if (dt.Rows[i]["DATA_TYPE"].ToString().ToUpper() == "DATE")
                    {
                        ht_pre.Add(dt.Rows[i]["COLUMN_NAME"].ToString(), dt.Rows[i]["DATA_TYPE"].ToString());
                    }
                }
            }

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
                    if (ht_pre.Contains(item.Key.ToString()))
                    {
                        sql += " " + item.Key.ToString() + "=to_date(:" + item.Key.ToString() + ",'yyyy-MM-dd HH24:mi:ss'),";
                    }
                    else
                    {
                        sql += " " + item.Key.ToString() + "=:" + item.Key.ToString() + ",";
                    }
                    paras.Add(new OracleParameter()
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
        /// 向表中更新数据并根据ht里面的键值对作为关键字更新(关键字默认不参与更新)
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="ht">键值表</param>
        /// <param name="keys">关键字集合</param>
        /// <param name="isKeyAttend">关键字是否参与到更新中</param>
        /// <returns>是否更新成功</returns>
        public bool UpdateData(string tableName, Hashtable ht, List<string> keys, bool isKeyAttend = false)
        {
            DataTable dt = GetDataTable(string.Format(" select DATA_TYPE,COLUMN_NAME from user_tab_cols where table_name='{0}'", tableName.ToUpper()));
            Hashtable ht_pre = new Hashtable();
            if (dt.Rows.Count > 0)
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    if (dt.Rows[i]["DATA_TYPE"].ToString().ToUpper() == "DATE")
                    {
                        ht_pre.Add(dt.Rows[i]["COLUMN_NAME"].ToString(), dt.Rows[i]["DATA_TYPE"].ToString());
                    }
                }
            }

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
                    if (ht_pre.Contains(item.Key.ToString()))
                    {
                        sql += " " + item.Key.ToString() + "=to_date(:" + item.Key.ToString() + ",'yyyy-MM-dd HH24:mi:ss'),";
                    }
                    else
                    {
                        sql += " " + item.Key.ToString() + "=:" + item.Key.ToString() + ",";
                    }
                    IDbDataParameter para = new OracleParameter()
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
                if (ht_pre.Contains(item.ToUpper().ToString()))
                {
                    sql += " and " + item + "=to_date(:" + item + ",'yyyy-MM-dd HH24:mi:ss')";
                }
                else
                {
                    sql += " and " + item + "=:" + item;
                }
                paras.Add(new OracleParameter()
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
            OracleCommand cmd = new OracleCommand(strSql, (OracleConnection)conn);
            if (IsTran)
            {
                cmd.Transaction = (OracleTransaction)tran;
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
            OracleCommand cmd = new OracleCommand(strSql, (OracleConnection)conn);
            cmd.Parameters.AddRange(paraArr);
            if (IsTran)
            {
                cmd.Transaction = (OracleTransaction)tran;
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
        /// 返回查到的第一行第一列的字符串值(该方法将调用GetFirstColumn,并将返回的对象转换成字符串)
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
        /// 返回查到的第一行第一列的字符串值(该方法将调用GetFirstColumn,并将返回的对象转换成字符串)
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
            OracleCommand cmd = new OracleCommand(strSql, (OracleConnection)conn);
            if (IsTran)
            {
                cmd.Transaction = (OracleTransaction)tran;
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
            OracleCommand cmd = new OracleCommand(strSql, (OracleConnection)conn);
            cmd.Parameters.AddRange(paraArr);
            if (IsTran)
            {
                cmd.Transaction = (OracleTransaction)tran;
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
            OracleCommand cmd = new OracleCommand(strSql, (OracleConnection)conn);
            if (IsTran)
            {
                cmd.Transaction = (OracleTransaction)tran;
            }
            if (!IsOpen)
            {
                conn.Open();
                IsOpen = true;
            }
            OracleDataAdapter adp = new OracleDataAdapter(cmd);
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
            OracleCommand cmd = new OracleCommand(strSql, (OracleConnection)conn);
            cmd.Parameters.AddRange(paraArr);
            if (IsTran)
            {
                cmd.Transaction = (OracleTransaction)tran;
            }
            if (!IsOpen)
            {
                conn.Open();
                IsOpen = true;
            }
            OracleDataAdapter adp = new OracleDataAdapter(cmd);
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
            string sql = string.Format("select count(1) from user_tab_cols t where t.table_name='{0}' and t.column_name='{1}'", tableName, columnName);
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
            string sql = string.Format("select count(1) from user_tables t where t.TABLE_NAME ='{0}'", tableName);
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
            string sql = string.Format(@"select * from (select inner__.*,ROWNUM RNO__ from({0} {1}) inner__) outer__ where outer__.RNO__ between {2} and {3} ", selectSql, strOrder, (PageIndex - 1) * PageSize + 1, PageSize * PageIndex);
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

        //public List<DataView> ShowViews()
        //{
        //    throw new NotImplementedException();
        //}

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
            return false;
        }

        public int TruncateTable(string TableName)
        {
            return ExecuteSql("delete from [" + TableName + "]");
        }

        private string GenerateInserSql(IList<string> maplist, string TableName, DataRow row)
        {
            var names = new StringBuilder();
            var values = new StringBuilder();
            bool first = true;
            char quote = '"';

            foreach (string c in maplist)
            {
                if (!first)
                {
                    names.Append(",");
                    values.Append(",");
                }
                names.Append(c);
                values.Append("\"" + row[c].ToString().Replace(quote.ToString(), string.Concat(quote, quote)) + "\"");
                first = false;
            }

            string sql = string.Format("INSERT INTO {0} ({1}) VALUES ({2})", TableName, names, values);
            return sql;
        }

        public bool BulkInsert(string tableName, DataTable dt)
        {
            using OracleConnection connection = new OracleConnection(ConnectionString);
            connection.Open();
            OracleCommand cmd = new OracleCommand
            {
                Connection = connection
            };
            OracleTransaction tx = connection.BeginTransaction();
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
                return true;
            }
            catch (Exception E)
            {
                tx.Rollback();
                throw new Exception(E.Message);
            }
        }

        #region 存储过程操作

        /// <summary>
        /// 执行存储过程 返回SqlDataReader ( 注意：调用该方法后，一定要对SqlDataReader进行Close )
        /// </summary>
        /// <param name="storedProcName">存储过程名</param>
        /// <param name="parameters">存储过程参数</param>
        /// <returns>OracleDataReader</returns>
        public OracleDataReader RunProcedure(string storedProcName, IDataParameter[] parameters)
        {
            OracleConnection connection = new OracleConnection(ConnectionString);
            OracleDataReader returnReader;
            connection.Open();
            OracleCommand command = BuildQueryCommand(connection, storedProcName, parameters);
            command.CommandType = CommandType.StoredProcedure;
            returnReader = command.ExecuteReader(CommandBehavior.CloseConnection);
            return returnReader;
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
            using (OracleConnection connection = new OracleConnection(ConnectionString))
            {
                DataSet dataSet = new DataSet();
                connection.Open();
                OracleDataAdapter sqlDA = new OracleDataAdapter();
                sqlDA.SelectCommand = BuildQueryCommand(connection, storedProcName, parameters);
                sqlDA.Fill(dataSet, tableName);
                connection.Close();
                return dataSet;
            }
        }

        /// <summary>
        /// 构建 OracleCommand 对象(用来返回一个结果集，而不是一个整数值)
        /// </summary>
        /// <param name="connection">数据库连接</param>
        /// <param name="storedProcName">存储过程名</param>
        /// <param name="parameters">存储过程参数</param>
        /// <returns>OracleCommand</returns>
        private static OracleCommand BuildQueryCommand(OracleConnection connection, string storedProcName, IDataParameter[] parameters)
        {
            OracleCommand command = new OracleCommand(storedProcName, connection);
            command.CommandType = CommandType.StoredProcedure;
            if (parameters != null)
            {
                foreach (OracleParameter parameter in parameters)
                {
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
            using OracleConnection connection = new OracleConnection(ConnectionString);
            int result;
            connection.Open();
            OracleCommand command = BuildIntCommand(connection, storedProcName, parameters);
            rowsAffected = command.ExecuteNonQuery();
            result = (int)command.Parameters["ReturnValue"].Value;
            connection.Close();
            return result;
        }

        public int RunProcedure(string storedProcName)
        {
            using OracleConnection connection = new OracleConnection(ConnectionString);
            int result;
            connection.Open();
            OracleCommand command = BuildIntCommand(connection, storedProcName, null);
            command.CommandTimeout = 100000;
            result = command.ExecuteNonQuery();
            connection.Close();
            return result;
        }

        IDataReader IDbAccess.RunProcedure(string storedProcName, IDataParameter[] parameters)
        {
            OracleConnection connection = new OracleConnection(ConnectionString);
            OracleDataReader returnReader;
            connection.Open();
            OracleCommand command = BuildQueryCommand(connection, storedProcName, parameters);
            command.CommandType = CommandType.StoredProcedure;
            returnReader = command.ExecuteReader(CommandBehavior.CloseConnection);
            return returnReader;
        }

        /// <summary>
        /// 创建 OracleCommand 对象实例(用来返回一个整数值)
        /// </summary>
        /// <param name="storedProcName">存储过程名</param>
        /// <param name="parameters">存储过程参数</param>
        /// <returns>OracleCommand 对象实例</returns>
        private static OracleCommand BuildIntCommand(OracleConnection connection, string storedProcName, IDataParameter[] parameters)
        {
            OracleCommand command = BuildQueryCommand(connection, storedProcName, parameters);
            command.Parameters.Add(new OracleParameter("ReturnValue",
                 OracleDbType.Int32, 4, ParameterDirection.ReturnValue,
                false, 0, 0, string.Empty, DataRowVersion.Default, null));
            return command;
        }

        public bool BulkInsert(string tableName, DataTable dt, IList<string> maplist)
        {
            using OracleConnection connection = new OracleConnection(ConnectionString);
            connection.Open();
            OracleCommand cmd = new OracleCommand
            {
                Connection = connection
            };
            OracleTransaction tx = connection.BeginTransaction();
            cmd.Transaction = tx;
            try
            {
                foreach (DataRow r in dt.Rows)
                {
                    cmd.CommandText = GenerateInserSql(maplist, tableName, r);
                    cmd.ExecuteNonQuery();
                }

                tx.Commit();
                return true;
            }
            catch (Exception E)
            {
                tx.Rollback();
                connection.Close();
                throw new Exception(E.Message);
            }
        }

        #endregion 存储过程操作
    }
}