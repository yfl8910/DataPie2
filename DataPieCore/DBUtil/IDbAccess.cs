using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;

namespace DBUtil
{
    /// <summary>
    /// 数据库访问对象
    /// </summary>
    public interface IDbAccess : IDisposable
    {
        /// <summary>
        /// 是否保持连接不断开
        /// </summary>
        bool IsKeepConnect { get; set; }

        /// <summary>
        /// 事物对象
        /// </summary>
        IDbTransaction tran { get; set; }

        /// <summary>
        /// 连接字符串
        /// </summary>
        string ConnectionString { get; set; }

        /// <summary>
        /// 连接对象
        /// </summary>
        IDbConnection conn { get; set; }

        /// <summary>
        /// 数据库类型
        /// </summary>
        DataBaseType DataBaseType { get; set; }

        /// <summary>
        /// 记录是否打开了连接,防止多次打开连接
        /// </summary>
        bool IsOpen { get; set; }

        /// <summary>
        /// 记录是否开启了事务,防止多次开启事务
        /// </summary>
        bool IsTran { get; set; }

        /// <summary>
        /// 打开连接测试
        /// </summary>
        Result OpenTest();

        /// <summary>
        /// 当前数据库使用的参数的前缀符号
        /// </summary>
        string paraPrefix { get; }

        /// <summary>
        /// 创建参数
        /// </summary>
        /// <returns>针对当前数据库类型的空参数对象</returns>
        IDbDataParameter CreatePara();

        /// <summary>
        /// 创建具有名称和值的参数
        /// <para>示例：iDb.CreatePara("id",id);</para>
        /// </summary>
        /// <param name="name">参数名,不用加前缀</param>
        /// <param name="value">参数值</param>
        /// <returns>针对当前数据库类型的参数对象</returns>
        IDbDataParameter CreatePara(string name, object value);

        /// <summary>
        /// 根据指定日期范围生成过滤字符串
        /// <para>
        /// 示例：iDb.GetDateFilter("CreateTime", DateTime.Now.ToString("yyyy-MM-dd"), DateTime.Now.ToString("yyyy-MM-dd 23:59:59"), false, true);//返回 "and CreateTime&gt;'2019-11-13' and CreateTime&lt;='2019-11-13 23:59:59'"
        /// </para>
        /// </summary>
        /// <param name="dateColumn">要进行过滤的字段名称</param>
        /// <param name="minDate">最小日期</param>
        /// <param name="MaxDate">最大日期</param>
        /// <param name="isMinInclude">最小日期是否包含</param>
        /// <param name="isMaxInclude">最大日期是否包含</param>
        /// <returns>返回生成的过滤字符串</returns>
        string GetDateFilter(string dateColumn, string minDate, string MaxDate, bool isMinInclude, bool isMaxInclude);

        /// <summary>
        /// 执行sql语句
        /// </summary>
        /// <param name="strSql">要执行的sql语句</param>
        /// <returns>受影响的行数</returns>
        int ExecuteSql(string strSql);

        /// <summary>
        /// 执行sql语句
        /// <para>
        /// 示例：iDb.ExecuteSql("update User set Sta=1 where Id=" + iDb.paraPrefix + "id", iDb.CreatePara("id", 1));
        /// </para>
        /// </summary>
        /// <param name="strSql">要执行的sql语句</param>
        /// <param name="paramArr">sql参数数组</param>
        /// <returns>受影响的行数</returns>
        int ExecuteSql(string strSql, params IDataParameter[] paramArr);

        /// <summary>
        /// 执行多个sql语句
        /// </summary>
        /// <param name="strSql">多个SQL语句的数组</param>
        /// <returns></returns>
        void ExecuteSql(string[] strSql);

        /// <summary>
        /// 执行多个sql语句
        /// <para>
        /// 示例：iDb.ExecuteSql(new string[] { "update User set Sta=1 where Id=" + iDb.paraPrefix + "id", "", "update User setSta=2 wher Id=" + iDb.paraPrefix + "id" }, new IDataParameter[] { iDb.CreatePara("id", 1) }, new IDataParameter[] { iDb.CreatePara("id", 2) });
        /// </para>
        /// </summary>
        /// <param name="strSql">多个SQL语句的数组</param>
        /// <param name="paraArrs">多个SQL语句的参数对应的二维数组</param>
        /// <returns></returns>
        void ExecuteSql(string[] strSql, params IDataParameter[][] paraArrs);

        /// <summary>
        /// 向一个表中添加一行数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="reader">reader</param>
        /// <returns>返回是受影响的行数</returns>
        bool BulkInsert(string tableName, IDataReader reader);

        /// <summary>
        /// 向一个表中添加一行数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="dt">数据表</param>
        /// <returns>返回是受影响的行数</returns>
        bool BulkInsert(string tableName, DataTable dt);

        public bool BulkInsert(string tableName, DataTable dt, IList<string> maplist);

        /// <summary>
        /// 向一个表中添加一行数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="ht">列名和值的键值对</param>
        /// <returns>是否插入成功</returns>
        bool AddData(string tableName, Hashtable ht);

        /// <summary>
        /// 向一个表中添加一行数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="dic">列名和值的键值对</param>
        /// <returns>是否插入成功</returns>
        bool AddData(string tableName, Dictionary<string, object> dic);

        /// <summary>
        /// 根据键值表中的数据向表中更新数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="ht">键值表</param>
        /// <param name="filterStr">过滤条件以and开头</param>
        /// <returns>是否更新成功</returns>
        bool UpdateData(string tableName, Hashtable ht, string filterStr);

        /// <summary>
        /// 根据键值表中的数据向表中更新数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="dic">键值表</param>
        /// <param name="filterStr">过滤条件以and开头</param>
        /// <returns>是否更新成功</returns>
        bool UpdateData(string tableName, Dictionary<string, object> dic, string filterStr);

        /// <summary>
        /// 根据键值表中的数据向表中更新数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="ht">键值表</param>
        /// <param name="filterStr">过滤条件以and开头</param>
        /// <param name="paraArr">过滤条件中的参数数组</param>
        /// <returns>是否更新成功</returns>
        bool UpdateData(string tableName, Hashtable ht, string filterStr, params IDbDataParameter[] paraArr);

        /// <summary>
        /// 根据键值表中的数据向表中更新数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="dic">键值表</param>
        /// <param name="filterStr">过滤条件以and开头</param>
        /// <param name="paraArr">过滤条件中的参数数组</param>
        /// <returns>是否更新成功</returns>
        bool UpdateData(string tableName, Dictionary<string, object> dic, string filterStr, params IDbDataParameter[] paraArr);

        /// <summary>
        /// 向表中更新数据并根据指定的键值对作为关键字更新(关键字默认不参与更新)
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="ht">键值表</param>
        /// <param name="keys">关键字集合</param>
        /// <param name="isKeyAttend">关键字是否参与到更新中</param>
        /// <returns>是否更新成功</returns>
        bool UpdateData(string tableName, Hashtable ht, List<string> keys, bool isKeyAttend = false);

        /// <summary>
        /// 向表中更新数据并根据指定的键值对作为关键字更新(关键字默认不参与更新)
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="dic">键值表</param>
        /// <param name="keys">关键字集合</param>
        /// <param name="isKeyAttend">关键字是否参与到更新中</param>
        /// <returns>是否更新成功</returns>
        bool UpdateData(string tableName, Dictionary<string, object> dic, List<string> keys, bool isKeyAttend = false);

        /// <summary>
        /// 根据键值表中的数据向表中添加或更新数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="ht">键值表</param>
        /// <param name="filterStr">过滤条件以and开头</param>
        /// <returns>是否更新成功</returns>
        bool UpdateOrAdd(string tableName, Hashtable ht, string filterStr);

        /// <summary>
        /// 根据键值表中的数据向表中添加或更新数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="dic">键值表</param>
        /// <param name="filterStr">过滤条件以and开头</param>
        /// <returns>是否更新成功</returns>
        bool UpdateOrAdd(string tableName, Dictionary<string, object> dic, string filterStr);

        /// <summary>
        /// 根据键值表中的数据向表中添加或更新数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="ht">键值表</param>
        /// <param name="filterStr">过滤条件以and开头</param>
        /// <param name="paraArr">过滤条件中的参数数组</param>
        /// <returns>是否更新成功</returns>
        bool UpdateOrAdd(string tableName, Hashtable ht, string filterStr, params IDbDataParameter[] paraArr);

        /// <summary>
        /// 根据键值表中的数据向表中添加或更新数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="dic">键值表</param>
        /// <param name="filterStr">过滤条件以and开头</param>
        /// <param name="paraArr">过滤条件中的参数数组</param>
        /// <returns>是否更新成功</returns>
        bool UpdateOrAdd(string tableName, Dictionary<string, object> dic, string filterStr, params IDbDataParameter[] paraArr);

        /// <summary>
        /// 向表中添加或更新数据并根据里面的键值对作为关键字更新(关键字默认不参与更新)
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="ht">键值表</param>
        /// <param name="keys">关键字集合</param>
        /// <param name="isKeyAttend">关键字是否参与到更新中</param>
        /// <returns>是否更新成功</returns>
        bool UpdateOrAdd(string tableName, Hashtable ht, List<string> keys, bool isKeyAttend = false);

        /// <summary>
        /// 向表中添加或更新数据并根据里面的键值对作为关键字更新(关键字默认不参与更新)
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="dic">键值表</param>
        /// <param name="keys">关键字集合</param>
        /// <param name="isKeyAttend">关键字是否参与到更新中</param>
        /// <returns>是否更新成功</returns>
        bool UpdateOrAdd(string tableName, Dictionary<string, object> dic, List<string> keys, bool isKeyAttend = false);

        /// <summary>
        /// 删除一行
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="strFilter">过滤条件</param>
        /// <returns>返回受影响的行数</returns>
        int DeleteTableRow(string tableName, string strFilter);

        /// <summary>
        /// 删除一行
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="strFilter">过滤条件</param>
        /// <param name="paraArr">过滤条件中的参数集合</param>
        /// <returns>返回受影响的行数</returns>
        int DeleteTableRow(string tableName, string strFilter, params IDbDataParameter[] paraArr);

        /// <summary>
        /// 返回查到的第一行第一列的值
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回查到的第一行第一列的值</returns>
        object GetFirstColumn(string strSql);

        /// <summary>
        /// 返回查到的第一行第一列的值
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="paraArr">sql语句参数</param>
        /// <returns>返回查到的第一行第一列的值</returns>
        object GetFirstColumn(string strSql, params IDbDataParameter[] paraArr);

        /// <summary>
        /// 返回查到的第一行第一列的字符串值(该方法将调用GetFirstColumn,并将返回的对象转换成字符串)
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="isReturnNull">false:查询结果为null就返回""否则返回null</param>
        /// <returns>返回查到的第一行第一列的值</returns>
        string GetFirstColumnString(string strSql, bool isReturnNull = false);

        /// <summary>
        /// 返回查到的第一行第一列的字符串值(该方法将调用GetFirstColumn,并将返回的对象转换成字符串)
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="paraArr">sql语句中的参数数组</param>
        /// <param name="isReturnNull">false:查询结果为null就返回""否则返回null</param>
        /// <returns>返回查到的第一行第一列的值</returns>
        string GetFirstColumnString(string strSql, IDbDataParameter[] paraArr, bool isReturnNull = false);

        /// <summary>
        /// 返回查到的第一行第一列的字符串值(该方法将调用GetFirstColumn,并将返回的对象转换成字符串)
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="paraArr">sql语句中的参数数组</param>
        /// <param name="isReturnNull">false:查询结果为null就返回""否则返回null</param>
        /// <returns>返回查到的第一行第一列的值</returns>
        string GetFirstColumnString(string strSql, bool isReturnNull = false, params IDbDataParameter[] paraArr);

        /// <summary>
        /// 获取阅读器
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回阅读器</returns>
        IDataReader GetDataReader(string strSql);

        /// <summary>
        /// 获取阅读器
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="paraArr">sql语句参数</param>
        /// <returns>返回阅读器</returns>
        IDataReader GetDataReader(string strSql, params IDbDataParameter[] paraArr);

        /// <summary>
        /// 返回查询结果的数据集
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回的查询结果集</returns>
        DataSet GetDataSet(string strSql);

        /// <summary>
        /// 返回查询结果的数据集
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="paraArr">sql语句参数</param>
        /// <returns>返回的查询结果集</returns>
        DataSet GetDataSet(string strSql, params IDbDataParameter[] paraArr);

        /// <summary>
        /// 返回查询结果的数据表
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回的查询结果集</returns>
        DataTable GetDataTable(string strSql);

        /// <summary>
        /// 返回查询结果的数据表
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="paraArr">sql语句参数</param>
        /// <returns>返回的查询结果集</returns>
        DataTable GetDataTable(string strSql, params IDbDataParameter[] paraArr);

        /// <summary>
        /// 开启事务
        /// </summary>
        void BeginTrans();

        /// <summary>
        /// 提交事务
        /// </summary>
        void Commit();

        /// <summary>
        /// 回滚事务
        /// </summary>
        void Rollback();

        /// <summary>
        /// 获得分页的查询语句
        /// </summary>
        /// <param name="selectSql">查询子句,如: "select id,name,age from student where age>18"</param>
        /// <param name="strOrder">排序子句,如: "order by age desc"</param>
        /// <param name="PageSize">分页大小,如:10</param>
        /// <param name="PageIndex">当前页索引,如:1(第一页)</param>
        /// <returns></returns>
        string GetSqlForPageSize(string selectSql, string strOrder, int PageSize, int PageIndex);

        /// <summary>
        /// 判断指定表或视图中是否有某一列
        /// </summary>
        /// <param name="tableName">表或视图名</param>
        /// <param name="columnName">列名</param>
        /// <returns>返回列是否存在</returns>
        bool JudgeColumnExist(string tableName, string columnName);

        /// <summary>
        /// 返回表或视图是否存在
        /// </summary>
        /// <param name="tableName">表或视图名</param>
        /// <returns>返回表或视图是否存在</returns>
        bool JudgeTableOrViewExist(string tableName);

        /// <summary>
        /// 返回所有的表
        /// </summary>
        /// <returns>返回所有的表</returns>
        public List<TableStruct> ShowTables();

        /// <summary>
        /// 返回所有的视图
        /// </summary>
        /// <returns>返回所有的视图</returns>
        public List<string> ShowViews();

        public List<Proc> GetProcs();

        /// <summary>
        /// 根据当前的数据库类型和连接字符串创建一个新的数据库操作对象
        /// </summary>
        /// <returns></returns>
        IDbAccess CreateNewIDB();

        int TruncateTable(string TableName);

        /// <summary>
        /// 执行存储过程，返回影响行数
        /// </summary>
        public int RunProcedure(string storedProcName);

        /// <summary>
        /// 执行存储过程，返回影响的行数
        /// </summary>
        /// <param name="storedProcName">存储过程名</param>
        /// <param name="parameters">存储过程参数</param>
        /// <param name="rowsAffected">影响的行数</param>
        /// <returns></returns>
        public int RunProcedure(string storedProcName, IDataParameter[] parameters, out int rowsAffected);

        /// <summary>
        /// 执行存储过程，返回SqlDataReader ( 注意：调用该方法后，一定要对SqlDataReader进行Close )
        /// </summary>
        /// <param name="storedProcName">存储过程名</param>
        /// <param name="parameters">存储过程参数</param>
        /// <returns>SqlDataReader</returns>
        public IDataReader RunProcedure(string storedProcName, IDataParameter[] parameters);

        /// <summary>
        /// 执行存储过程
        /// </summary>
        /// <param name="storedProcName">存储过程名</param>
        /// <param name="parameters">存储过程参数</param>
        /// <param name="tableName">DataSet结果中的表名</param>
        /// <returns>DataSet</returns>
        public DataSet RunProcedure(string storedProcName, IDataParameter[] parameters, string tableName);

        public List<string> GetDataBaseInfo();

        public DbSchema ShowDbSchema();
    }
}