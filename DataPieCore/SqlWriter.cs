using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DBUtil;
using System.IO;

namespace DataPieCore
{
  public class SqlWriter
    {
        public static string DBtype { get; set; } = "SQLSERVER";

    
        public static string WriteSelect(TableStruct tableOrView)
        {
            StringWriter writer = new StringWriter();

            writer.Write("SELECT");
            writer.WriteLine();
            for (int i = 0; i < tableOrView.Columns.Count; i++)
            {
                writer.Write("\t");
                writer.Write(MakeSqlFriendly(tableOrView.Columns[i].Name));
                if (i < tableOrView.Columns.Count - 1)
                {
                    writer.Write(",");
                    writer.WriteLine();
                }
            }

            writer.WriteLine();
            writer.Write("FROM {0}", MakeSqlFriendly(tableOrView.Name));
            writer.WriteLine();

            return writer.ToString();
        }

  
        public static string WriteSelect(TableStruct tableOrView, int top)
        {
            StringWriter writer = new StringWriter();
            if (DBtype == "SQLSERVER")
            {
                writer.Write("SELECT TOP " + top + "  ");
            }
            else {
                writer.Write("SELECT " );
            }
            writer.WriteLine();
            for (int i = 0; i < tableOrView.Columns.Count; i++)
            {
                //writer.Write("\t");
                writer.Write(MakeSqlFriendly(tableOrView.Columns[i].Name));
                if (i < tableOrView.Columns.Count - 1)
                {
                    writer.Write(",");
                    //writer.WriteLine();
                }
            }

            writer.WriteLine();
            writer.Write("FROM {0}", MakeSqlFriendly(tableOrView.Name));
            if (DBtype == "MYSQL" || DBtype == "SQLITE") { writer.Write(" LIMIT " + top + "  ;"); }

            writer.WriteLine();

            return writer.ToString();
        }


        public static string WriteSelectCount(TableStruct tableOrView)
        {
            StringWriter writer = new StringWriter();

            writer.Write("SELECT COUNT(*) FROM {0}", MakeSqlFriendly(tableOrView.Name));
            writer.WriteLine();

            return writer.ToString();
        }

        public static string WriteUpdate( TableStruct tableOrView)
        {
            StringWriter writer = new StringWriter();

            writer.Write("UPDATE ");
            writer.WriteLine(MakeSqlFriendly(tableOrView.Name));
            writer.WriteLine("SET");

            // get all columns that are "writable" excluding keys that are not auto generated
            var writableColumns = tableOrView.Columns.FindAll(c =>  !c.IsPrimaryKey);
            for (int i = 0; i < writableColumns.Count; i++)
            {
                var column = writableColumns[i];
                writer.Write("\t{0} = {1}", MakeSqlFriendly(column.Name), column.FinalType);
                if (i < writableColumns.Count - 1)
                {
                    writer.Write(",");
                    writer.WriteLine();
                }
            }

            writer.WriteLine();
            writer.WriteLine("WHERE");

            for (int i = 0; i < tableOrView.Columns.Count; i++)
            {
                var column = tableOrView.Columns[i];
                if (column.IsPrimaryKey == true )
                {
                    writer.Write("\t{0} = ", MakeSqlFriendly(column.Name));
                    writer.Write(" /*value:{0}*/ ", column.Name);
                    writer.WriteLine();
                }

            }


            writer.WriteLine();

            return writer.ToString();

        }


        public static string WriteDelete(TableStruct tableOrView)
        {
            StringWriter writer = new StringWriter();

            writer.WriteLine("DELETE FROM");
            writer.Write("\t");
            writer.WriteLine(MakeSqlFriendly(tableOrView.Name));
            writer.WriteLine("WHERE");

            for (int i = 0; i < tableOrView.Columns.Count; i++)
            {
                var column = tableOrView.Columns[i];
                if (column.IsPrimaryKey==true )
                {
                    writer.Write("\t{0} = ", MakeSqlFriendly(column.Name));
                    writer.Write(" /*value:{0}*/", column.Name);
                    writer.WriteLine();
                }

            }

            writer.WriteLine();

            return writer.ToString();

        }

        public static string WriteSummary(Column column)
        {
            StringWriter writer = new StringWriter();

            writer.Write("{0} ({1} ", MakeSqlFriendly(column.Name), column.FinalType);

            if (!column.IsNullable)
            {
                writer.Write("not ");
            }

            writer.Write("null)");

            return writer.ToString();

        }

        /// <summary>
        /// 	Attempts to convert a database object name to it's "bracketed for", e.g. "Name" -> "[Name]".
        /// </summary>
        /// <param name = "name">The name of the object.</param>
        /// <returns>The SQL friendly conversion.</returns>
        public static string MakeSqlFriendly(string name)
        {
            if (name == null)
            {
                return string.Empty;
            }

            if (!name.StartsWith("[") && (!name.StartsWith("`")))
            {
                switch (DBtype)
                {
                    case "SQLSERVER":

                        return string.Concat("[", name, "]");

                    case "MYSQL":

                        return string.Concat("`", name, "`");

                    default:
                        return string.Concat("`", name, "`");

                }


            }

            return name;
        }


    }
}
