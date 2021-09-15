using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;

namespace DBUtil
{
    public partial class OracleDbAccess : IDbAccess
    {
        public DbSchema ShowDbSchema()
        {
            DbSchema dbs = new DbSchema
            {
                ConnectionStrings = ConnectionString,
                Dbtype = "ORACLE",
                DbTables = ShowTables(),
                DbViews = ShowViews(),
                DbProcs = GetProcs()
            };
            return dbs;
        }

        /// <summary>
        /// 获得所有表,注意返回的集合中的表模型中只有表名
        /// </summary>
        /// <returns></returns>
        public List<TableStruct> ShowTables()
        {
            DataSet ds = GetDataSet("select TABLE_NAME from user_tables");
            TableStruct tbl = null;
            List<TableStruct> list = new List<TableStruct>();
            for (int i = 0; i < ds.Tables[0].Rows.Count; i++)
            {
                tbl = new TableStruct();
                tbl.Name = ds.Tables[0].Rows[i][0].ToString();
                list.Add(tbl);
            }
            return list;
        }

        public List<Proc> GetProcs()
        {
            throw new NotImplementedException();
        }

        public List<string> GetDataBaseInfo()
        {
            throw new NotImplementedException();
        }

        public List<string> ShowViews()
        {
            var List = new List<string>();
            DataTable dt = GetSchema("views");
            int num = dt.Rows.Count;
            if (dt.Rows.Count > 0)
            {
                foreach (DataRow _DataRowItem in dt.Rows)
                {
                    List.Add(_DataRowItem["VIEW_NAME"].ToString());
                }
            }
            return List;
        }

        public DataTable GetSchema(string collectionName)
        {
            using (OracleConnection connection = new OracleConnection(ConnectionString))
            {
                DataTable dt = new DataTable();
                try
                {
                    dt.Clear();
                    connection.Open();
                    dt = connection.GetSchema(collectionName);
                }
                catch
                {
                    dt = null;
                }
                return dt;
            }
        }
    }
}