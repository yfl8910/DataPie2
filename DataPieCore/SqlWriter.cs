using System;
using System.Linq;
using DBUtil;

namespace DataPieCore
{
    public static class SqlWriter
    {
        public static string WriteSelect(TableStruct table, string databaseType, int? rowLimit = null)
            => SqlQueryBuilder.BuildQuery(table.Columns.Select(column => column.Name).ToArray(),
                table.Name, databaseType, rowLimit);

        public static string WriteSelectCount(TableStruct table, string databaseType)
            => $"SELECT COUNT(*) FROM {SqlQueryBuilder.QuoteIdentifier(table.Name, databaseType)}";

        public static string WriteUpdate(TableStruct table, string databaseType)
        {
            string assignments = string.Join("," + Environment.NewLine, table.Columns
                .Where(column => !column.IsPrimaryKey)
                .Select(column => $"\t{SqlQueryBuilder.QuoteIdentifier(column.Name, databaseType)} = {column.FinalType}"));
            return $"UPDATE {SqlQueryBuilder.QuoteIdentifier(table.Name, databaseType)}{Environment.NewLine}SET{Environment.NewLine}{assignments}{Environment.NewLine}WHERE{Environment.NewLine}{BuildKeyPredicate(table, databaseType)}";
        }

        public static string WriteDelete(TableStruct table, string databaseType)
            => $"DELETE FROM {SqlQueryBuilder.QuoteIdentifier(table.Name, databaseType)}{Environment.NewLine}WHERE{Environment.NewLine}{BuildKeyPredicate(table, databaseType)}";

        private static string BuildKeyPredicate(TableStruct table, string databaseType)
            => string.Join(Environment.NewLine + "AND ", table.Columns
                .Where(column => column.IsPrimaryKey)
                .Select(column => $"{SqlQueryBuilder.QuoteIdentifier(column.Name, databaseType)} = /*value:{column.Name}*/"));

        public static string WriteSummary(Column column, string databaseType)
            => $"{SqlQueryBuilder.QuoteIdentifier(column.Name, databaseType)} ({column.FinalType} {(column.IsNullable ? "" : "not ")}null)";
    }
}
