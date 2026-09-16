using System;
using System.Collections.Generic;
using System.Linq;

namespace DataPieCore
{
    public static class SqlQueryBuilder
    {
        public static string BuildQuery(IList<string> columns, string tableName, string databaseType, int? rowLimit = null)
        {
            ArgumentNullException.ThrowIfNull(columns);
            if (rowLimit < 0) throw new ArgumentOutOfRangeException(nameof(rowLimit));
            string columnList = string.Join(", ", columns.Select(column => QuoteIdentifier(column, databaseType)));
            return BuildSelect(columnList, tableName, databaseType, rowLimit);
        }

        public static string BuildSelectAll(string tableName, string databaseType)
            => BuildSelect("*", tableName, databaseType, null);

        private static string BuildSelect(string columns, string tableName, string databaseType, int? rowLimit)
        {
            bool isSqlServer = IsSqlServer(databaseType);
            string top = isSqlServer && rowLimit.HasValue ? $"TOP {rowLimit.Value} " : "";
            string limit = !isSqlServer && rowLimit.HasValue ? $" LIMIT {rowLimit.Value}" : "";
            return $"SELECT {top}{columns} FROM {QuoteIdentifier(tableName, databaseType)}{limit}";
        }

        public static string QuoteIdentifier(string name, string databaseType)
        {
            ArgumentNullException.ThrowIfNull(name);
            return IsSqlServer(databaseType)
                ? "[" + name.Replace("]", "]]") + "]"
                : "`" + name.Replace("`", "``") + "`";
        }

        private static bool IsSqlServer(string databaseType)
        {
            if (string.Equals(databaseType, "SQLSERVER", StringComparison.OrdinalIgnoreCase)) return true;
            if (string.Equals(databaseType, "SQLITE", StringComparison.OrdinalIgnoreCase)) return false;
            throw new ArgumentException("Unsupported database type.", nameof(databaseType));
        }
    }
}
