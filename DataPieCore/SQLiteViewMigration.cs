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
            var names = schema.Tables.Select(table => table.Name)
                .Concat(schema.ViewDefinitions.Select(view => view.ViewName))
                .GroupBy(name => name, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(group => group.Key, group => group.Count(), StringComparer.OrdinalIgnoreCase);
            var objects = new HashSet<string>(
                schema.Tables.Select(table => table.TableSchemaName + "." + table.Name)
                    .Concat(schema.ViewDefinitions.Where(view => names[view.ViewName] == 1)
                        .Select(view => view.SchemaName + "." + view.ViewName)),
                StringComparer.OrdinalIgnoreCase);

            foreach (var view in schema.ViewDefinitions)
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
            if (allowWrites) NormalizeDelete(body);
            if (allowWrites) NormalizeJoinedUpdate(body);
            ConvertTop(body);
            ConvertPivot(body);
            var numericExpressions = new HashSet<string>(StringComparer.Ordinal);
            ConvertFunctions(body, numericExpressions, numericVariables);

            for (int i = 0; i < body.Count; i++)
            {
                if (body[i].Equals("ISNULL", StringComparison.OrdinalIgnoreCase) &&
                    i + 1 < body.Count && body[i + 1] == "(" && (i == 0 || body[i - 1] != "."))
                    body[i] = "IFNULL";
                // '+' and COLLATE can silently change meaning between the two database engines.
                bool Numeric(string token) => decimal.TryParse(token, System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out _) || numericVariables?.Contains(token) == true || numericExpressions.Contains(token);
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

        private static void ConvertFunctions(List<string> tokens, HashSet<string> numericExpressions, HashSet<string> numericVariables)
        {
            for (int i = 0; i + 1 < tokens.Count; i++)
            {
                string name = tokens[i].ToUpperInvariant();
                if (!new[] { "LEFT", "LEN", "SUBSTRING", "YEAR", "MONTH", "DAY", "ISNULL" }.Contains(name) ||
                    tokens[i + 1] != "(" || (i > 0 && tokens[i - 1] == ".")) continue;
                var arguments = new List<List<string>> { new List<string>() };
                int depth = 0;
                int end = i + 2;
                for (; end < tokens.Count; end++)
                {
                    string token = tokens[end];
                    if (token == ")" && depth == 0) break;
                    if (token == "," && depth == 0) { arguments.Add(new List<string>()); continue; }
                    if (token == "(") depth++;
                    if (token == ")") depth--;
                    arguments[arguments.Count - 1].Add(token);
                }
                int count = name == "LEFT" || name == "ISNULL" ? 2 : name == "SUBSTRING" ? 3 : 1;
                if (end == tokens.Count || arguments.Count != count || arguments.Any(arg => arg.Count == 0))
                    throw new NotSupportedException($"Invalid {name} function arguments.");
                if (arguments.SelectMany(arg => arg).Any(token => token.Equals("SELECT", StringComparison.OrdinalIgnoreCase)))
                    throw new NotSupportedException("Subqueries inside converted scalar functions require explicit conversion.");
                foreach (var argument in arguments)
                {
                    ConvertFunctions(argument, numericExpressions, numericVariables);
                    bool Numeric(string token) => decimal.TryParse(token, System.Globalization.NumberStyles.Float,
                        System.Globalization.CultureInfo.InvariantCulture, out _) || numericExpressions.Contains(token) || numericVariables?.Contains(token) == true;
                    for (int j = 0; j < argument.Count; j++)
                        if (argument[j].Equals("COLLATE", StringComparison.OrdinalIgnoreCase) ||
                            (argument[j] == "+" && !(j > 0 && j + 1 < argument.Count && Numeric(argument[j - 1]) && Numeric(argument[j + 1]))))
                            throw new NotSupportedException("Explicit conversion is required for '+' or COLLATE inside function arguments.");
                }
                var args = arguments.Select(arg => "(" + string.Join(" ", arg) + ")").ToArray();
                // SQLite's negative substring lengths have different semantics; force a runtime error instead.
                const string invalid = "abs(-9223372036854775808)";
                string expression;
                if (name == "ISNULL")
                    expression = $"IFNULL({args[0]}, {args[1]})";
                else if (name == "LEN")
                    expression = $"length(rtrim({args[0]}, ' '))";
                else if (name == "LEFT")
                    expression = $"CASE WHEN {args[0]} IS NULL OR {args[1]} IS NULL THEN NULL WHEN {args[1]} < 0 THEN {invalid} ELSE substr({args[0]}, 1, {args[1]}) END";
                else if (name == "SUBSTRING")
                    expression = $"CASE WHEN {args[0]} IS NULL OR {args[1]} IS NULL OR {args[2]} IS NULL THEN NULL WHEN {args[2]} < 0 THEN {invalid} ELSE substr({args[0]}, max(1, {args[1]}), max(0, {args[2]} + min(0, {args[1]} - 1))) END";
                else
                {
                    string format = name == "YEAR" ? "%Y" : name == "MONTH" ? "%m" : "%d";
                    string value = $"strftime('{format}', {args[0]})";
                    expression = $"CASE WHEN {args[0]} IS NULL THEN NULL WHEN {value} IS NULL THEN {invalid} ELSE CAST({value} AS INTEGER) END";
                }
                expression = "(" + expression + ")";
                bool NumericArgument(List<string> argument) => argument.Count == 1 &&
                    (numericExpressions.Contains(argument[0]) || numericVariables?.Contains(argument[0]) == true ||
                     decimal.TryParse(argument[0], System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out _));
                if (name == "LEN" || name == "YEAR" || name == "MONTH" || name == "DAY" ||
                    (name == "ISNULL" && arguments.All(NumericArgument))) numericExpressions.Add(expression);
                tokens.RemoveRange(i, end - i + 1);
                tokens.Insert(i, expression);
            }
        }

        private static void NormalizeJoinedUpdate(List<string> body)
        {
            if (body.Count < 3 || !body[0].Equals("UPDATE", StringComparison.OrdinalIgnoreCase) ||
                !body[2].Equals("SET", StringComparison.OrdinalIgnoreCase)) return;
            int from = body.FindIndex(3, token => token.Equals("FROM", StringComparison.OrdinalIgnoreCase));
            if (from < 0) return;
            int join = body.FindIndex(from + 1, token => token.Equals("JOIN", StringComparison.OrdinalIgnoreCase));
            if (join < 0) return;
            int targetEnd = join;
            if (body[join - 1].Equals("INNER", StringComparison.OrdinalIgnoreCase)) targetEnd--;
            string alias = body[1];
            if (!body[targetEnd - 1].Equals(alias, StringComparison.OrdinalIgnoreCase)) return;
            int tableEnd = targetEnd - 1;
            if (body[tableEnd - 1].Equals("AS", StringComparison.OrdinalIgnoreCase)) tableEnd--;
            var target = body.GetRange(from + 1, tableEnd - from - 1);
            if (!(target.Count == 1 || (target.Count == 3 && target[1] == ".")))
                throw new NotSupportedException("Joined UPDATE requires a simple target table.");
            int on = body.FindIndex(join + 1, token => token.Equals("ON", StringComparison.OrdinalIgnoreCase));
            if (on < 0 || body.Skip(join + 1).Any(token => token.Equals("JOIN", StringComparison.OrdinalIgnoreCase)))
                throw new NotSupportedException("Only one INNER JOIN is supported for alias UPDATE.");
            int where = body.FindIndex(on + 1, token => token.Equals("WHERE", StringComparison.OrdinalIgnoreCase));
            var assignments = body.GetRange(3, from - 3);
            int depth = 0;
            for (int i = 0; i < assignments.Count; i++)
            {
                if (depth == 0 && (i == 0 || assignments[i - 1] == ",") && i + 3 < assignments.Count &&
                    assignments[i].Equals(alias, StringComparison.OrdinalIgnoreCase) && assignments[i + 1] == "." && assignments[i + 3] == "=")
                    assignments.RemoveRange(i, 2);
                if (assignments[i] == "(") depth++;
                if (assignments[i] == ")") depth--;
            }
            var rewritten = new List<string> { "UPDATE" };
            rewritten.AddRange(target);
            rewritten.AddRange(new[] { "AS", alias, "SET" });
            rewritten.AddRange(assignments);
            rewritten.Add("FROM");
            rewritten.AddRange(body.GetRange(join + 1, on - join - 1));
            rewritten.AddRange(new[] { "WHERE", "(" });
            rewritten.AddRange(body.GetRange(on + 1, (where < 0 ? body.Count : where) - on - 1));
            rewritten.Add(")");
            if (where >= 0)
            {
                rewritten.AddRange(new[] { "AND", "(" });
                rewritten.AddRange(body.Skip(where + 1));
                rewritten.Add(")");
            }
            body.Clear();
            body.AddRange(rewritten);
        }

        private static void NormalizeDelete(List<string> body)
        {
            if (!body[0].Equals("DELETE", StringComparison.OrdinalIgnoreCase) ||
                (body.Count > 1 && body[1].Equals("FROM", StringComparison.OrdinalIgnoreCase))) return;
            const string identifier = @"^(?:\[(?:\]\]|[^\]])+\]|""(?:""""|[^""])+""|[\p{L}_#][\p{L}\p{N}_$#]*)$";
            int end = 2;
            if (body.Count < end || !Regex.IsMatch(body[1], identifier) || body[1].Equals("TOP", StringComparison.OrdinalIgnoreCase))
                throw new NotSupportedException("Only DELETE table [WHERE ...] can omit FROM.");
            if (body.Count > end && body[end] == ".")
            {
                if (body.Count <= end + 1 || !Regex.IsMatch(body[end + 1], identifier))
                    throw new NotSupportedException("Invalid DELETE target.");
                end += 2;
            }
            if (body.Count > end && !body[end].Equals("WHERE", StringComparison.OrdinalIgnoreCase))
                throw new NotSupportedException("DELETE aliases, joins, OUTPUT and cross-database targets require explicit conversion.");
            body.Insert(1, "FROM");
        }

        private static void ConvertTop(List<string> body)
        {
            if (body[0].Equals("INSERT", StringComparison.OrdinalIgnoreCase))
            {
                int insertDepth = 0;
                for (int i = 1; i < body.Count; i++)
                {
                    if (body[i] == "(") insertDepth++;
                    if (body[i] == ")") insertDepth--;
                    if (insertDepth != 0 || !body[i].Equals("SELECT", StringComparison.OrdinalIgnoreCase)) continue;
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
            if (remaining.Any(token => new[] { "SUM", "COUNT", "AVG", "MIN", "MAX", "OVER" }
                .Contains(token, StringComparer.OrdinalIgnoreCase)))
                throw new NotSupportedException("Only SUM(ISNULL(pivotColumn, 0)) aggregates can be collapsed safely.");
            int order = remaining.FindIndex(token => token.Equals("ORDER", StringComparison.OrdinalIgnoreCase));
            for (int i = 0; i < (order < 0 ? remaining.Count : order); i++)
            {
                if (i > 0 && remaining[i - 1].Equals("AS", StringComparison.OrdinalIgnoreCase)) continue;
                if (columns.Any(column => Unquote(column).Equals(Unquote(remaining[i]), StringComparison.OrdinalIgnoreCase)))
                    throw new NotSupportedException("PIVOT columns must only occur inside SUM(ISNULL(column, 0)); filters and grouping must use source dimensions.");
                if (new[] { pivot.Groups["key"].Value, pivot.Groups["value"].Value }
                    .Any(column => Unquote(column).Equals(Unquote(remaining[i]), StringComparison.OrdinalIgnoreCase)))
                    throw new NotSupportedException("PIVOT key and value columns are unavailable outside the pivot aggregate.");
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
