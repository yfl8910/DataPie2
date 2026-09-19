using System.Collections.Generic;

namespace DBUtil
{
    /// <summary>
    /// 数据库Schema信息
    /// </summary>
    ///
    public class DbSchema
    {
        public string Name { set; get; }

        public List<TableStruct> Tables { get; set; } = new();

        public List<string> ViewNames { get; set; } = new();

        public List<ViewSchema> ViewDefinitions { get; set; } = new();

        public List<Proc> Procedures { get; set; } = new();

    }

    public class TableStruct
    {
        public string TableSchemaName { set; get; }
        public string Name { set; get; }

        public List<Column> Columns { get; set; } = new();


        public List<ForeignKeySchema> ForeignKeys { get; set; } = new();
    }

    public class Column
    {
        public string Name { set; get; }
        public string Type { set; get; }

        public string FinalType
        {
            get
            {
                if (Type==null) {
                    return "";
                }
                else if ((Type.Contains("binary")
                    || Type.Contains("char")
                    || Type == "datetime2"
                    || Type == "datetimeoffset"
                    || Type == "decimal"
                    || Type == "numeric"
                    || Type == "time")
                    && (!Type.Contains("(")))
                {
                    //采取的是Type和MaxLength分离的方式
                    return Type + "(" + (MaxLength == -1 ? "max" : MaxLength.ToString()) + ")";
                }
                else
                {
                    return Type;
                }
            }
        }


        public bool IsIdentity { set; get; }

        public bool IsNullable { set; get; }

        public bool IsPrimaryKey { set; get; }

        public string Default { set; get; }

        public int MaxLength { set; get; }


        public bool IsUnique { set; get; }
    }

    public class Proc
    {
        public string ScriptWarning { get; set; }
        public string SchemaName { get; set; }
        public string Name { set; get; }
        public string CreateSql { set; get; }

    }

    public class ForeignKeySchema
    {

        public List<string> ColumnNames { get; set; } = new();

        public string ReferencedTableName { get; set; }

        public List<string> ReferencedColumnNames { get; set; } = new();
        public string OnDelete { get; set; } = "NO ACTION";
        public string OnUpdate { get; set; } = "NO ACTION";

    }

    public class ViewSchema
    {
        public string SchemaName;
        public string ViewName;

        public string ViewSQL;
    }


}
