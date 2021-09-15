using System;
using System.Data.SqlClient;
using System.IO;

namespace DBUtil
{
    /// <summary>
    /// 根据不同的数据库创建数据库访问对象:IDBAccess
    /// </summary>
    public class IDBFactory
    {
        /// <summary>
        /// 创建IDB对象
        /// <para>
        /// 示例：DBUtil.IDbAccess iDb = DBUtil.IDBFactory.CreateIDB("Data Source=.;Initial Catalog=;User ID=sa;Password=sa;","SQLSERVER");
        /// </para>
        /// </summary>
        /// <param name="connStr">
        /// <para>连接字符串:</para>
        /// <para>SQLSERVER:   Data Source=.;Initial Catalog=test;User ID=sa;Password=xx;</para>
        /// <para>ORACLE:   Data Source=test;Password=sys123;User ID=sys;DBA Privilege=SYSDBA;</para>
        /// <para>MYSQL:   Data Source=localhost;Initial Catalog=test;User ID=root;Password=xxxx;</para>
        /// <para>POSTGRESQL:   Server=localhost;Port=5432;UserId=postgres;Password=xxxx;Database=test</para>
        /// <para>SQLITE:   Data Source=D:\demo.db;</para>
        /// </param>
        /// <param name="DBType">数据库类型:SQLSERVER、ORACLE、MYSQL、SQLITE、ACCESS、POSTGRESQL</param>
        /// <returns></returns>
        public static IDbAccess CreateIDB(string connStr, string DBType)
        {
            DBType = (DBType ?? "").ToUpper();
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
            else if (DBType == "MYSQL")
            {
                //使用单独一个方法,防止在下面代码访问不到的情况下仍会因没有mysql组件而报错
                return CreateMySql(connStr);
            }
            //else if (DBType == "ORACLE")
            //{
            //    //使用单独一个方法,防止在下面代码访问不到的情况下仍会因没有oracle组件而报错
            //    return CreateOracle(connStr);
            //}
            else if (DBType == "SQLITE")
            {
                //使用单独一个方法,防止在下面代码访问不到的情况下仍会因没有sqlite组件而报错
                return CreateSQLite(connStr);
            }
            else if (DBType == "POSTGRESQL")
            {
                //使用单独一个方法,防止在下面代码访问不到的情况下仍会因没有postgresql组件而报错
                return CreatePostgreSql(connStr);
            }
            else
            {
                throw new Exception("暂不支持这种(" + DBType + ")数据库!");
            }
        }

        /// <summary>
        /// 创建IDB对象,注意.netcore中不支持oledb，这里也不再支持oledb、access
        /// <para>
        /// 示例：DBUtil.IDbAccess iDb = DBUtil.IDBFactory.CreateIDB("Data Source=.;Initial Catalog=test;User ID=sa;Password=sa;","SQLSERVER");
        /// </para>
        /// </summary>
        /// <returns></returns>
        public static IDbAccess CreateIDB(string connStr, DataBaseType DBType)
        {
            string dbtype = DBType.ToString();
            return CreateIDB(connStr, dbtype);
        }

        //private static IDbAccess CreateOracle(string connStr)
        //{
        //    Oracle.ManagedDataAccess.Client.OracleConnection conn = new Oracle.ManagedDataAccess.Client.OracleConnection(connStr);
        //    IDbAccess iDb = new OracleDbAccess()
        //    {
        //        conn = conn,
        //        ConnectionString = connStr,
        //        DataBaseType = DataBaseType.ORACLE
        //    };
        //    return iDb;
        //}

        private static IDbAccess CreateMySql(string connStr)
        {
            MySql.Data.MySqlClient.MySqlConnection conn = new MySql.Data.MySqlClient.MySqlConnection(connStr);
            IDbAccess iDb = new MySqlDbAccess()
            {
                conn = conn,
                ConnectionString = connStr,
                DataBaseType = DataBaseType.MYSQL
            };
            return iDb;
        }

        private static IDbAccess CreatePostgreSql(string connStr)
        {
            Npgsql.NpgsqlConnection conn = new Npgsql.NpgsqlConnection(connStr);
            IDbAccess iDb = new PostgreSqlDbAccess()
            {
                conn = conn,
                ConnectionString = connStr,
                DataBaseType = DataBaseType.POSTGRESQL
            };
            return iDb;
        }

        /// <summary>
        /// 创建Sqlite数据库文件,如果已存在就报错
        /// <para>
        /// 示例:IDBFactory.CreateSQLiteDB(@"D:\demo.db");
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
        /// 示例：IDBFactory.GetSQLiteConnectionString(@"D:\demo.db");//返回"Data Source=D:\demo.db"
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
        /// 示例：IDBFactory.GetSQLiteConnectionString(@"D:\demo.db","123456");//返回"Data Source=D:\demo.db;Password=123456"
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

            if (File.Exists("data.db") == false)
            {
                System.Data.SQLite.SQLiteConnection.CreateFile("data.db");
            }

            IDbAccess iDb = new SQLiteDbAccess()
            {
                conn = conn,
                ConnectionString = connStr,
                DataBaseType = DataBaseType.SQLITE
            };

            if (iDb.ShowTables().Count == 0)
            {
                iDb.ExecuteSql(" CREATE TABLE Dbinfo(Id INTEGER PRIMARY KEY AUTOINCREMENT, Dbname  varchar (50) NOT NULL, ConnectionStrings varchar (255)  NOT NULL, Dbtype varchar (20)  NOT NULL); ");
            }

            return iDb;
        }
    }
}