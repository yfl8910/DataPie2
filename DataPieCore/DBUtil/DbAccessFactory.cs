using System;
using System.Data.SqlClient;
using System.IO;

namespace DBUtil
{
    /// <summary>
    /// 根据不同的数据库创建数据库访问对象:IDBAccess
    /// </summary>
    public static class DbAccessFactory
    {
        /// <summary>
        /// 创建IDB对象
        /// <para>
        /// 示例：DBUtil.IDbAccess iDb = DBUtil.DbAccessFactory.Create("Data Source=.;Initial Catalog=;User ID=sa;Password=sa;","SQLSERVER");
        /// </para>
        /// </summary>
        /// <param name="connStr">
        /// <para>连接字符串:</para>
        /// <para>SQLSERVER:   Data Source=.;Initial Catalog=test;User ID=sa;Password=xx;</para>
        /// <para>SQLITE:   Data Source=D:\demo.db;</para>
        /// </param>
        /// <param name="DBType">数据库类型:SQLSERVER、SQLITE</param>
        /// <returns></returns>
        public static IDbAccess Create(string connStr, string DBType)
        {
            DBType = (DBType ?? "").ToUpperInvariant();
            if (DBType == "SQLSERVER")
            {
                SqlConnection conn = new SqlConnection(connStr);
                IDbAccess iDb = new SqlServerDbAccess()
                {
                    conn = conn,
                    ConnectionString = connStr,
                    DataBaseType = DataBaseType.SQLSERVER
                };
                return iDb;
            }
            else if (DBType == "SQLITE")
            {
                //使用单独一个方法,防止在下面代码访问不到的情况下仍会因没有sqlite组件而报错
                return CreateSQLite(connStr);
            }
            else
            {
                throw new Exception("暂不支持这种(" + DBType + ")数据库!");
            }
        }

        /// <summary>
        /// 创建IDB对象,注意.netcore中不支持oledb，这里也不再支持oledb、access
        /// <para>
        /// 示例：DBUtil.IDbAccess iDb = DBUtil.DbAccessFactory.Create("Data Source=.;Initial Catalog=test;User ID=sa;Password=sa;","SQLSERVER");
        /// </para>
        /// </summary>
        /// <returns></returns>
        public static IDbAccess Create(string connStr, DataBaseType DBType)
        {
            string dbtype = DBType.ToString();
            return Create(connStr, dbtype);
        }

        /// <summary>
        /// 创建Sqlite数据库文件,如果已存在就报错
        /// <para>
        /// 示例:DbAccessFactory.CreateSQLiteDB(@"D:\demo.db");
        /// </para>
        /// </summary>
        /// <param name="absPath">文件绝对路径</param>
        public static void CreateSQLiteDB(string absPath)
        {
            if (File.Exists(absPath))
            {
                throw new Exception("要创建的数据库文件已存在，请核对：" + absPath);
            }
            System.Data.SQLite.SQLiteConnection.CreateFile(absPath);
        }

        /// <summary>
        /// 获取Sqlite数据库连接方式
        /// <para>
        /// 示例：DbAccessFactory.GetSQLiteConnectionString(@"D:\demo.db");//返回"Data Source=D:\demo.db"
        /// </para>
        /// </summary>
        /// <param name="absPath">文件绝对路径</param>
        /// <returns></returns>
        public static string GetSQLiteConnectionString(string absPath)
        {
            return GetSQLiteConnectionString(absPath, null);
        }

        /// <summary>
        /// 获取Sqlite数据库连接方式
        /// <para>
        /// 示例：DbAccessFactory.GetSQLiteConnectionString(@"D:\demo.db","123456");//返回"Data Source=D:\demo.db;Password=123456"
        /// </para>
        /// </summary>
        /// <param name="absPath">文件绝对路径</param>
        /// <param name="pwd">sqlite文件密码</param>
        /// <returns></returns>
        public static string GetSQLiteConnectionString(string absPath, string pwd)
        {
            string str;
            if (string.IsNullOrWhiteSpace(pwd))
            {
                str = "Data Source=" + absPath;
            }
            else
            {
                str = "Data Source=" + absPath + ";Password=" + pwd;
            }
            return str;
        }

        private static IDbAccess CreateSQLite(string connStr)
        {
            System.Data.SQLite.SQLiteConnection conn = new System.Data.SQLite.SQLiteConnection(connStr);

            IDbAccess iDb = new SQLiteDbAccess()
            {
                conn = conn,
                ConnectionString = connStr,
                DataBaseType = DataBaseType.SQLITE
            };

            return iDb;
        }
    }
}
