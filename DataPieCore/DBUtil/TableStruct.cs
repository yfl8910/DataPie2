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
        public string ConnectionStrings { set; get; }
        public string Dbtype { set; get; }

        public List<TableStruct> DbTables = new List<TableStruct>();

        public List<string> DbViews = new List<string>();

        public List<ViewSchema> DbViews2 = new List<ViewSchema>();

        public List<Proc> DbProcs = new List<Proc>();

        public List<string> DbList = new List<string>();
    }

    public class TableStruct
    {
        public string TableSchemaName { set; get; }
        public string Name { set; get; }
        public string Desc { set; get; }
        public string PrimaryKey { set; get; }

        public List<Column> Columns = new List<Column>();

        public List<Constraint> Constraints = new List<Constraint>();

        public List<Trigger> Triggers = new List<Trigger>();

        public List<Index> Indexs = new List<Index>();

        public List<ForeignKeySchema> ForeignKeys = new List<ForeignKeySchema>();
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

        public string IdentityStr { set; get; }

        public string FinalIdentity
        {
            get
            {
                if (!string.IsNullOrWhiteSpace(IdentityStr))
                {
                    return IdentityStr;
                }
                return IsIdentity ? "identity(" + (Start == 0 ? 1 : Start).ToString() + "," + (Incre == 0 ? 1 : Incre).ToString() + ")" : "";
            }
        }

        public string Desc { set; get; }

        public bool IsIdentity { set; get; }

        public bool IsNullable { set; get; }

        public bool IsPrimaryKey { set; get; }

        public string Default { set; get; }

        public int MaxLength { set; get; }

        public int Start { set; get; }

        public int Incre { set; get; }

        public bool IsUnique { set; get; }
    }

    public class Constraint
    {
        public string Name { set; get; }
        public string Type { set; get; }
        public string DelType { set; get; }
        public string UpdateType { set; get; }
        public string Keys { set; get; }
        public string RefStr { set; get; }
        public string Remark { set; get; }
    }

    public class Trigger
    {
        public string Name { set; get; }
        public string Type { set; get; }
    }

    public class Index
    {
        public string Name { set; get; }
        public string Desc { set; get; }
        public string Keys { set; get; }
    }

    public class Proc
    {
        public string Name { set; get; }
        public string LastUpdate { set; get; }
        public string CreateSql { set; get; }

        public List<Procparam> Param { set; get; }
    }

    public class Procparam
    {
        public string Name { set; get; }
        public string Type { set; get; }
        public string Length { set; get; }
    }

    public class Func
    {
        public string Name { set; get; }
        public string Type { set; get; }
        public string LastUpdate { set; get; }
        public string CreateSql { set; get; }
    }

    public class ForeignKeySchema
    {
        public string TableName;

        public string ColumnName;

        public string ForeignTableName;

        public string ForeignColumnName;

        public bool CascadeOnDelete;

        public bool IsNullable;
    }

    public class ViewSchema
    {
        public string ViewName;

        public string ViewSQL;
    }


}