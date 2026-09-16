using System;
using System.Collections.Generic;
using System.Data;
using DBUtil;

namespace DataPieCore
{
    public static class QueryPreview
    {
        // Do not rewrite arbitrary SQL: it may contain CTEs, batches or procedures.
        // Bound the client result and cancel unread results once the limit is reached.
        public static DataTable Load(IDbAccess access, string sql, int limit, out bool truncated)
        {
            if (limit < 1) throw new ArgumentOutOfRangeException(nameof(limit));
            if (access.conn.State != ConnectionState.Open) access.conn.Open();
            using var command = access.conn.CreateCommand();
            command.CommandText = sql;
            command.CommandTimeout = 30;
            using var reader = command.ExecuteReader();
            var table = new DataTable();
            var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < reader.FieldCount; i++)
            {
                string original = reader.GetName(i);
                if (string.IsNullOrEmpty(original)) original = "Column" + (i + 1);
                string name = original;
                for (int suffix = 1; !names.Add(name); suffix++) name = original + "_" + suffix;
                table.Columns.Add(name, reader.GetFieldType(i));
            }
            truncated = false;
            table.BeginLoadData();
            try
            {
                while (table.Rows.Count < limit && reader.Read())
                {
                    var values = new object[reader.FieldCount];
                    reader.GetValues(values);
                    table.Rows.Add(values);
                }
                truncated = table.Rows.Count == limit && reader.Read();
                if (truncated) command.Cancel();
            }
            finally { table.EndLoadData(); }
            return table;
        }
    }
}
