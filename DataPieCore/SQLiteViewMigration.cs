using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Text.RegularExpressions;
using DBUtil;

namespace DataPieCore
{
    internal static class SQLiteViewMigration
    {
        // Keep quoted identifiers and string literals intact when rewriting object references.
        private static readonly Regex Tokens = new Regex(
            @"--[^\r\n]*|/\*[\s\S]*?\*/|[Nn]?'(?:''|[^'])*'|\[(?:\]\]|[^\]])*\]|""(?:""""|[^""])*""|[\p{L}_#@][\p{L}\p{N}_$#@]*|\d+(?:\.\d+)?|<>|!=|<=|>=|\S",
            RegexOptions.Compiled);

        public static List<string> Create(SQLiteConnection connection, DbSchema schema)
        {
            var errors = new List<string>();
            var created = new List<ViewSchema>();
            var names = schema.DbTables.Select(table => table.Name)
                .Concat(schema.DbViews2.Select(view => view.ViewName))
                .GroupBy(name => name, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(group => group.Key, group => group.Count(), StringComparer.OrdinalIgnoreCase);
            var objects = new HashSet<string>(
                schema.DbTables.Select(table => table.TableSchemaName + "." + table.Name)
                    .Concat(schema.DbViews2.Where(view => names[view.ViewName] == 1)
                        .Select(view => view.SchemaName + "." + view.ViewName)),
                StringComparer.OrdinalIgnoreCase);

            foreach (var view in schema.DbViews2)
            {
                try
                {
                    if (names[view.ViewName] != 1)
                        throw new NotSupportedException("Name conflicts with another table or view.");
                    using var command = new SQLiteCommand(ConvertDefinition(view, objects), connection);
                    command.ExecuteNonQuery();
                    created.Add(view);
                }
                catch (Exception ex) when (ex is SQLiteException || ex is NotSupportedException)
                {
                    errors.Add($"{view.SchemaName}.{view.ViewName}: {ex.Message}");
                }
            }

            // Validate after all definitions exist; repeat after removals to catch dependent views.
            bool removed;
            do
            {
                removed = false;
                foreach (var view in created.ToArray())
                {
                    try
                    {
                        using var command = new SQLiteCommand($"SELECT * FROM {Quote(view.ViewName)} LIMIT 0", connection);
                        using var reader = command.ExecuteReader();
                    }
                    catch (SQLiteException ex)
                    {
                        using var drop = new SQLiteCommand($"DROP VIEW {Quote(view.ViewName)}", connection);
                        drop.ExecuteNonQuery();
                        created.Remove(view);
                        errors.Add($"{view.SchemaName}.{view.ViewName}: {ex.Message}");
                        removed = true;
                    }
                }
            } while (removed);
            return errors;
        }

        private static string ConvertDefinition(ViewSchema view, HashSet<string> objects)
        {
            if (string.IsNullOrWhiteSpace(view.ViewSQL))
                throw new NotSupportedException("View definition is unavailable (permissions or encryption).");
            var tokens = Tokenize(view.ViewSQL);
            int position = 0;
            bool Take(string expected)
            {
                if (position >= tokens.Count || !tokens[position].Equals(expected, StringComparison.OrdinalIgnoreCase)) return false;
                position++;
                return true;
            }
            if (Take("CREATE"))
            {
                if (Take("OR") && !Take("ALTER")) throw new NotSupportedException("Unsupported view declaration.");
            }
            else if (!Take("ALTER")) throw new NotSupportedException("Expected CREATE VIEW or ALTER VIEW.");
            if (!Take("VIEW")) throw new NotSupportedException("Expected VIEW declaration.");
            if (position >= tokens.Count) throw new NotSupportedException("Missing view name.");
            position++;
            if (Take(".")) position++;
            var columns = new List<string>();
            if (Take("("))
            {
                while (position < tokens.Count && tokens[position] != ")")
                    columns.Add(tokens[position++]);
                if (!Take(")")) throw new NotSupportedException("Invalid view column list.");
            }
            if (!Take("AS")) throw new NotSupportedException("View options such as SCHEMABINDING are not supported.");
            var body = tokens.Skip(position).ToList();
            string query = ConvertQuery(body, objects);
            string columnList = columns.Count == 0 ? "" : " (" + string.Join(" ", columns) + ")";
            return $"CREATE VIEW {Quote(view.ViewName)}{columnList} AS {query}";
        }

        internal static List<string> Tokenize(string sql)
            => Tokens.Matches(sql).Cast<Match>().Select(match => match.Value)
                .Where(token => !token.StartsWith("--") && !token.StartsWith("/*"))
                .Select(token => token.StartsWith("N'", StringComparison.OrdinalIgnoreCase) ? token.Substring(1) : token)
                .ToList();

        internal static string ConvertQuery(List<string> body, HashSet<string> objects, bool allowWrites = false,
            HashSet<string> numericVariables = null)
        {
            if (body.LastOrDefault() == ";") body.RemoveAt(body.Count - 1);
            if (body.Count == 0 || body.Contains(";"))
                throw new NotSupportedException("Expected one view query.");
            ConvertTop(body);
            ConvertPivot(body);

            for (int i = 0; i < body.Count; i++)
            {
                if (body[i].Equals("ISNULL", StringComparison.OrdinalIgnoreCase) &&
                    i + 1 < body.Count && body[i + 1] == "(" && (i == 0 || body[i - 1] != "."))
                    body[i] = "IFNULL";
                // '+' and COLLATE can silently change meaning between the two database engines.
                bool Numeric(string token) => decimal.TryParse(token, System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out _) || numericVariables?.Contains(token) == true;
                if ((body[i] == "+" && !(i > 0 && i + 1 < body.Count && Numeric(body[i - 1]) && Numeric(body[i + 1]))) ||
                    body[i].Equals("COLLATE", StringComparison.OrdinalIgnoreCase))
                    throw new NotSupportedException("Explicit conversion is required for '+' or COLLATE.");
                if (!body[i].Equals("FROM", StringComparison.OrdinalIgnoreCase) &&
                    !body[i].Equals("JOIN", StringComparison.OrdinalIgnoreCase) &&
                    !(allowWrites && (body[i].Equals("UPDATE", StringComparison.OrdinalIgnoreCase) ||
                        body[i].Equals("INTO", StringComparison.OrdinalIgnoreCase)))) continue;
                if (i + 3 >= body.Count || body[i + 2] != ".") continue;
                string name = Unquote(body[i + 1]) + "." + Unquote(body[i + 3]);
                if (!objects.Contains(name) || (i + 4 < body.Count && body[i + 4] == "."))
                    throw new NotSupportedException($"Unmapped or cross-database reference: {name}");
                body[i + 1] = Quote(Unquote(body[i + 3]));
                body.RemoveRange(i + 2, 2);
            }
            return string.Join(" ", body);
        }

        private static void ConvertTop(List<string> body)
        {
            if (body[0].Equals("INSERT", StringComparison.OrdinalIgnoreCase))
            {
                int depth = 0;
                for (int i = 1; i < body.Count; i++)
                {
                    if (body[i] == "(") depth++;
                    if (body[i] == ")") depth--;
                    if (depth != 0 || !body[i].Equals("SELECT", StringComparison.OrdinalIgnoreCase)) continue;
                    var query = body.Skip(i).ToList();
                    ConvertTop(query);
                    body.RemoveRange(i, body.Count - i);
                    body.AddRange(query);
                    return;
                }
            }
            if (!body[0].Equals("SELECT", StringComparison.OrdinalIgnoreCase)) return;
            int start = 1;
            if (start < body.Count && (body[start].Equals("DISTINCT", StringComparison.OrdinalIgnoreCase) ||
                body[start].Equals("ALL", StringComparison.OrdinalIgnoreCase))) start++;
            if (start >= body.Count || !body[start].Equals("TOP", StringComparison.OrdinalIgnoreCase)) return;

            int end = start + 1;
            bool parenthesized = end < body.Count && body[end] == "(";
            if (parenthesized) end++;
            if (end >= body.Count || !Regex.IsMatch(body[end], @"^\d+$") || !long.TryParse(body[end], out _))
                throw new NotSupportedException("TOP requires a nonnegative integer constant.");
            string limit = body[end++];
            if (parenthesized)
            {
                if (end >= body.Count || body[end++] != ")")
                    throw new NotSupportedException("TOP expressions are not supported.");
            }
            if (end < body.Count && (body[end].Equals("PERCENT", StringComparison.OrdinalIgnoreCase) ||
                body[end].Equals("WITH", StringComparison.OrdinalIgnoreCase)))
                throw new NotSupportedException("TOP PERCENT and WITH TIES require explicit conversion.");

            // A trailing LIMIT applies to the whole compound query, unlike TOP in its first SELECT.
            int depth = 0;
            for (int i = end; i < body.Count; i++)
            {
                if (body[i] == "(") depth++;
                if (body[i] == ")") depth--;
                if (depth == 0 && new[] { "UNION", "INTERSECT", "EXCEPT", "LIMIT", "OFFSET" }
                    .Contains(body[i], StringComparer.OrdinalIgnoreCase))
                    throw new NotSupportedException("TOP with a compound query or pagination requires explicit conversion.");
            }
            body.RemoveRange(start, end - start);
            body.Add("LIMIT");
            body.Add(limit);
        }

        private static void ConvertPivot(List<string> body)
        {
            if (!body.Any(token => token.Equals("PIVOT", StringComparison.OrdinalIgnoreCase))) return;
            const string identifier = @"(?:\[(?:\]\]|[^\]])*\]|""(?:""""|[^""])*""|[\p{L}_][\p{L}\p{N}_$]*)";
            string sql = string.Join(" ", body);
            var pivot = Regex.Match(sql, @"\bFROM\s+(?<table>" + identifier + @"(?:\s*\.\s*" + identifier + @")?)\s+PIVOT\s*\(\s*SUM\s*\(\s*(?<value>" + identifier + @")\s*\)\s+FOR\s+(?<key>" + identifier + @")\s+IN\s*\(\s*(?<columns>" + identifier + @"(?:\s*,\s*" + identifier + @")*)\s*\)\s*\)\s+AS\s+" + identifier,
                RegexOptions.IgnoreCase);
            if (!pivot.Success || body.Count(token => token.Equals("PIVOT", StringComparison.OrdinalIgnoreCase)) != 1)
                throw new NotSupportedException("Only a single SUM PIVOT over a table followed by outer SUM aggregation is supported.");
            int select = body.FindIndex(token => token.Equals("SELECT", StringComparison.OrdinalIgnoreCase));
            string prefix = string.Join(" ", body.Take(select));
            string projection = sql.Substring(prefix.Length, pivot.Index - prefix.Length).Trim();
            string suffix = sql.Substring(pivot.Index + pivot.Length);
            if (!Regex.IsMatch(suffix, @"\bGROUP\s+BY\b", RegexOptions.IgnoreCase) ||
                Regex.IsMatch(suffix, @"\b(JOIN|UNION|INTERSECT|EXCEPT)\b", RegexOptions.IgnoreCase))
                throw new NotSupportedException("PIVOT conversion requires an outer GROUP BY without joins or compound queries.");
            var columns = Tokenize(pivot.Groups["columns"].Value).Where(token => token != ",").ToList();
            var expressions = new List<string>();
            foreach (string column in columns)
            {
                string pattern = @"SUM\s*\(\s*(?:ISNULL|IFNULL)\s*\(\s*" + Regex.Escape(column) + @"\s*,\s*0\s*\)\s*\)";
                string expression = "IFNULL ( SUM ( CASE WHEN " + pivot.Groups["key"].Value + " = '" +
                    Unquote(column).Replace("'", "''") + "' THEN " + pivot.Groups["value"].Value + " END ) , 0 )";
                string marker = "__datapie_pivot_" + expressions.Count + "__";
                if (sql.Contains(marker, StringComparison.Ordinal)) throw new NotSupportedException("Reserved PIVOT conversion identifier.");
                projection = Regex.Replace(projection, pattern, _ => marker, RegexOptions.IgnoreCase);
                expressions.Add(expression);
            }
            // Only additive pivot aggregates can be collapsed without reproducing implicit pivot grouping.
            var remaining = Tokenize(projection + " " + suffix);
            int order = remaining.FindIndex(token => token.Equals("ORDER", StringComparison.OrdinalIgnoreCase));
            for (int i = 0; i < (order < 0 ? remaining.Count : order); i++)
            {
                if (i > 0 && remaining[i - 1].Equals("AS", StringComparison.OrdinalIgnoreCase)) continue;
                if (columns.Any(column => Unquote(column).Equals(Unquote(remaining[i]), StringComparison.OrdinalIgnoreCase)))
                    throw new NotSupportedException("PIVOT columns must only occur inside SUM(ISNULL(column, 0)); filters and grouping must use source dimensions.");
            }
            for (int i = 0; i < expressions.Count; i++)
                projection = projection.Replace("__datapie_pivot_" + i + "__", expressions[i]);
            body.Clear();
            body.AddRange(Tokenize(prefix + " " + projection + " FROM " + pivot.Groups["table"].Value + " " + suffix));
        }

        private static string Quote(string name) => "\"" + name.Replace("\"", "\"\"") + "\"";
        private static string Unquote(string name)
            => name.StartsWith("[") ? name.Substring(1, name.Length - 2).Replace("]]", "]")
                : name.StartsWith("\"") ? name.Substring(1, name.Length - 2).Replace("\"\"", "\"") : name;
    }
}
