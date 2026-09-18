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
            var tokens = Tokens.Matches(view.ViewSQL).Cast<Match>().Select(match => match.Value)
                .Where(token => !token.StartsWith("--") && !token.StartsWith("/*"))
                .Select(token => token.StartsWith("N'", StringComparison.OrdinalIgnoreCase) ? token.Substring(1) : token)
                .ToList();
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
            if (body.LastOrDefault() == ";") body.RemoveAt(body.Count - 1);
            if (body.Count == 0 || body.Contains(";"))
                throw new NotSupportedException("Expected one view query.");
            ConvertTop(body);

            for (int i = 0; i < body.Count; i++)
            {
                // '+' and COLLATE can silently change meaning between the two database engines.
                if (body[i] == "+" || body[i].Equals("COLLATE", StringComparison.OrdinalIgnoreCase))
                    throw new NotSupportedException("Explicit conversion is required for '+' or COLLATE.");
                if (!body[i].Equals("FROM", StringComparison.OrdinalIgnoreCase) &&
                    !body[i].Equals("JOIN", StringComparison.OrdinalIgnoreCase)) continue;
                if (i + 3 >= body.Count || body[i + 2] != ".") continue;
                string name = Unquote(body[i + 1]) + "." + Unquote(body[i + 3]);
                if (!objects.Contains(name) || (i + 4 < body.Count && body[i + 4] == "."))
                    throw new NotSupportedException($"Unmapped or cross-database reference: {name}");
                body[i + 1] = Quote(Unquote(body[i + 3]));
                body.RemoveRange(i + 2, 2);
            }
            string columnList = columns.Count == 0 ? "" : " (" + string.Join(" ", columns) + ")";
            return $"CREATE VIEW {Quote(view.ViewName)}{columnList} AS {string.Join(" ", body)}";
        }

        private static void ConvertTop(List<string> body)
        {
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

        private static string Quote(string name) => "\"" + name.Replace("\"", "\"\"") + "\"";
        private static string Unquote(string name)
            => name.StartsWith("[") ? name.Substring(1, name.Length - 2).Replace("]]", "]")
                : name.StartsWith("\"") ? name.Substring(1, name.Length - 2).Replace("\"\"", "\"") : name;
    }
}
