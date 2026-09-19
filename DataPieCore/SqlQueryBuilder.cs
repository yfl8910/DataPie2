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

        internal static string QuoteSqlServerTableName(string name)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(name);
            var parts = new List<string>();
            int position = 0;
            while (position < name.Length)
            {
                while (position < name.Length && char.IsWhiteSpace(name[position])) position++;
                string part;
                if (position < name.Length && name[position] == '[')
                {
                    var value = new System.Text.StringBuilder();
                    position++;
                    bool closed = false;
                    while (position < name.Length)
                    {
                        char current = name[position++];
                        if (current != ']') { value.Append(current); continue; }
                        if (position < name.Length && name[position] == ']')
                        {
                            value.Append(']');
                            position++;
                        }
                        else { closed = true; break; }
                    }
                    if (!closed) throw new ArgumentException("Unclosed table identifier.", nameof(name));
                    part = value.ToString();
                    while (position < name.Length && char.IsWhiteSpace(name[position])) position++;
                }
                else
                {
                    int start = position;
                    while (position < name.Length && name[position] != '.') position++;
                    part = name.Substring(start, position - start).Trim();
                }
                if (part.Length == 0 || parts.Count == 4)
                    throw new ArgumentException("Invalid table identifier.", nameof(name));
                parts.Add(QuoteIdentifier(part, "SQLSERVER"));
                if (position == name.Length) break;
                if (name[position++] != '.' || position == name.Length)
                    throw new ArgumentException("Invalid table identifier.", nameof(name));
            }
            return string.Join(".", parts);
        }

        private static bool IsSqlServer(string databaseType)
        {
            if (string.Equals(databaseType, "SQLSERVER", StringComparison.OrdinalIgnoreCase)) return true;
            if (string.Equals(databaseType, "SQLITE", StringComparison.OrdinalIgnoreCase)) return false;
            throw new ArgumentException("Unsupported database type.", nameof(databaseType));
        }
    }
}
