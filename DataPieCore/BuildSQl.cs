using System.Collections.Generic;
using System.Text;

namespace DataPieCore
{
    public class BuildSQl
    {
        public static string BuildQuery(IList<string> col, string tablename, string DBtype)
        {
            string s1;
            string s2;
            switch (DBtype)
            {
                case "SQLSERVER":
                    s1 = "[";
                    s2 = "]";
                    break;

                case "MYSQL":
                    s1 = "`";
                    s2 = "`";
                    break;

                default:
                    s1 = "`";
                    s2 = "`";
                    break;
            }

            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT ");
            //sb.Append("\r\n");
            for (int i = 0; i < col.Count; i++)
            {
                sb.Append(s1 + col[i] + s2);
                if (i < col.Count - 1)
                    sb.Append(", ");
                //sb.Append("\r\n");
            }
            sb.Append(" FROM " + s1 + tablename + s2);
            return sb.ToString();
        }

        public static string BuildQuery(IList<string> col, string tablename, string DBtype, int top)
        {
            StringBuilder sb = new StringBuilder();
            string s1; string s2;
            sb.Append("SELECT ");
            switch (DBtype)
            {
                case "SQLSERVER":
                    s1 = "[";
                    s2 = "]";
                    sb.Append("TOP  " + top + "  ");
                    break;

                case "MYSQL":
                    s1 = "`";
                    s2 = "`";
                    break;

                default:
                    s1 = "`";
                    s2 = "`";
                    break;
            }

            //sb.Append("\r\n");

            for (int i = 0; i < col.Count; i++)
            {
                sb.Append(s1 + col[i] + s2);
                if (i < col.Count - 1)
                    sb.Append(", ");
                //sb.Append("\r\n");
            }

            sb.Append(" FROM " + s1 + tablename + s2);

            if (DBtype == "MYSQL"|| DBtype == "SQLITE") { sb.Append(" LIMIT " + top + "  ;"); }

            return sb.ToString();
        }

        public static string GetSQLfromTable(string TableName, string DBtype)
        {
            StringBuilder sb = new StringBuilder();
            string s1;
            string s2;

            switch (DBtype)
            {
                case "SQLSERVER":
                    s1 = "[";
                    s2 = "]";
                    break;

                case "MYSQL":
                    s1 = "`";
                    s2 = "`";
                    break;

                default:
                    s1 = "`";
                    s2 = "`";
                    break;
            }

            sb.Append("SELECT * FROM  " + s1 + TableName + s2);

            return sb.ToString();
        }
    }
}