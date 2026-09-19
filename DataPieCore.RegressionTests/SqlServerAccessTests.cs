using System.Data;
using System.Data.SqlClient;
using DataPieCore;
using DBUtil;

internal static class SqlServerAccessTests
{
    public static void Run()
    {
        void Check(bool condition) { if (!condition) throw new Exception("SQL Server access regression"); }
        Check(SqlQueryBuilder.QuoteSqlServerTableName("dbo.orders") == "[dbo].[orders]");
        Check(SqlQueryBuilder.QuoteSqlServerTableName("odd]table") == "[odd]]table]");
        Check(SqlQueryBuilder.QuoteSqlServerTableName("[sales.team].[odd]]table]") == "[sales.team].[odd]]table]");
        Check(SqlQueryBuilder.QuoteSqlServerTableName("[name.with.dots]") == "[name.with.dots]");
        foreach (string name in new[] { "", "dbo.", "dbo..orders", "[unfinished", "[table] suffix" })
        {
            bool rejected = false;
            try { SqlQueryBuilder.QuoteSqlServerTableName(name); }
            catch (ArgumentException) { rejected = true; }
            Check(rejected);
        }
        using var table = new DataTable();
        table.Columns.Add("ignored");
        table.Columns.Add("code");
        table.Columns.Add("id");
        using var reader = table.CreateDataReader();
        using var connection = new SqlConnection();
        using var bulk = new SqlBulkCopy(connection);
        SqlServerDbAccess.AddColumnMappings(bulk, reader, new HashSet<string>(new[] { "id", "code" }, StringComparer.Ordinal));
        Check(bulk.ColumnMappings.Count == 2);
        Check(bulk.ColumnMappings[0].SourceOrdinal == 1 && bulk.ColumnMappings[0].DestinationColumn == "code");
        Check(bulk.ColumnMappings[1].SourceOrdinal == 2 && bulk.ColumnMappings[1].DestinationColumn == "id");
        using var unmatched = new SqlBulkCopy(connection);
        bool noMatch = false;
        try { SqlServerDbAccess.AddColumnMappings(unmatched, reader, new HashSet<string> { "CODE" }); }
        catch (InvalidOperationException) { noMatch = true; }
        Check(noMatch);
        Console.WriteLine("PASS: SQL Server table quoting, ordinal mappings and zero-match rejection (offline).");
    }
}
