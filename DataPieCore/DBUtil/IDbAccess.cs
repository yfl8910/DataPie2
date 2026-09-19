using System;
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
        /// 执行sql语句
        /// </summary>
        /// <param name="strSql">要执行的sql语句</param>
        /// <returns>受影响的行数</returns>
        int ExecuteSql(string strSql);

        /// <summary>
        /// 向一个表中添加一行数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="reader">reader</param>
        /// <returns>返回是受影响的行数</returns>
        bool BulkInsert(string tableName, IDataReader reader);



        /// <summary>
        /// 获取阅读器
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回阅读器</returns>
        IDataReader GetDataReader(string strSql);

        /// <summary>
        /// 返回查询结果的数据表
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回的查询结果集</returns>
        DataTable GetDataTable(string strSql);


     

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
        IDbAccess CreateNewAccess();

        int TruncateTable(string TableName);

        /// <summary>
        /// 执行存储过程，返回影响行数
        /// </summary>
        public int RunProcedure(string storedProcName);

        /// <summary>
        /// 获取数据库信息
        /// </summary>
        public List<string> GetDataBaseInfo();

        /// <summary>
        /// 获取Schema信息
        /// </summary>
        public DbSchema ShowDbSchema();
    }
}
