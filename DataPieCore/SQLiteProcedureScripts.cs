using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using DBUtil;

namespace DataPieCore
{
    internal static class SQLiteProcedureScripts
    {
        private const string Header = "-- DataPie SQLite procedure: ";

        internal sealed class Parameter
        {
            public string Name { get; set; }
            public DbType Type { get; set; }
            public bool HasDefault { get; set; }
            public string DefaultValue { get; set; }
            public string Initializer { get; set; }
            public string SourceVariable { get; set; }
            public long Offset { get; set; }
        }

        internal sealed class Metadata
        {
            public string DatabaseFile { get; set; }
            public string Source { get; set; }
            public string Error { get; set; }
            public bool ReadOnly { get; set; }
            public List<string> Warnings { get; set; } = new List<string>();
            public List<Parameter> Parameters { get; set; } = new List<Parameter>();
            public List<Parameter> Locals { get; set; } = new List<Parameter>();
            public List<ConditionalAssignment> Assignments { get; set; } = new List<ConditionalAssignment>();
        }

        internal sealed class ConditionalAssignment
        {
            public string Left { get; set; }
            public string Operator { get; set; }
            public string Right { get; set; }
            public string Target { get; set; }
            public string Expression { get; set; }
        }

        internal sealed class Script
        {
            public Metadata Metadata { get; set; }
            public string Sql { get; set; }
        }

        internal static List<string> Export(SQLiteConnection connection, string sqlitePath, DbSchema schema)
        {
            string folder = Folder(sqlitePath);
            Directory.CreateDirectory(folder);
            var errors = new List<string>();
            var procedures = schema.DbProcs ?? new List<Proc>();
            var counts = procedures.GroupBy(proc => proc.Name, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(group => group.Key, group => group.Count(), StringComparer.OrdinalIgnoreCase);
            var objects = new HashSet<string>(schema.DbTables.Select(table => table.TableSchemaName + "." + table.Name)
                .Concat(schema.DbViews2.Select(view => view.SchemaName + "." + view.ViewName)), StringComparer.OrdinalIgnoreCase);
            var usedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var proc in procedures)
            {
                var metadata = new Metadata
                {
                    DatabaseFile = Path.GetFileName(sqlitePath),
                    Source = proc.SchemaName + "." + proc.Name
                };
                string name = counts[proc.Name] == 1 ? proc.Name : metadata.Source;
                string fileName = SafeName(name);
                if (!usedNames.Add(fileName))
                {
                    fileName += "_" + Hash(metadata.Source);
                    if (!usedNames.Add(fileName)) throw new InvalidOperationException("Duplicate procedure identity.");
                }
                string path = Path.Combine(folder, fileName + ".txt");
                string sql;
                try
                {
                    var statements = ConvertProcedure(proc.CreateSql, metadata, objects, schema);
                    // EXPLAIN compiles each statement without executing INSERT/UPDATE/DELETE.
                    for (int i = 0; i < statements.Count; i++)
                    {
                        string statement = statements[i];
                        if (statement.StartsWith("-- UNSUPPORTED:", StringComparison.Ordinal)) continue;
                        try
                        {
                            using var command = new SQLiteCommand("EXPLAIN " + statement, connection);
                            foreach (var parameter in metadata.Parameters.Concat(metadata.Locals))
                                command.Parameters.Add(new SQLiteParameter(parameter.Name, parameter.Type) { Value = DBNull.Value });
                            using var reader = command.ExecuteReader();
                            while (reader.Read()) { }
                        }
                        catch (SQLiteException ex)
                        {
                            metadata.Warnings.Add(ex.Message);
                            statements[i] = CommentSkipped(statement, ex.Message);
                        }
                    }
                    var executable = statements.Where(statement => !statement.StartsWith("-- UNSUPPORTED:", StringComparison.Ordinal)).ToList();
                    if (executable.Count == 0) throw new NotSupportedException("No executable statements remain. " + string.Join(" ", metadata.Warnings));
                    var referencedVariables = new HashSet<string>(executable.SelectMany(SQLiteViewMigration.Tokenize)
                        .Where(token => token.StartsWith("@", StringComparison.Ordinal)), StringComparer.OrdinalIgnoreCase);
                    foreach (var assignment in metadata.Assignments)
                        referencedVariables.UnionWith(SQLiteViewMigration.Tokenize(assignment.Left + " " + assignment.Right + " " + assignment.Target + " " + assignment.Expression)
                            .Where(token => token.StartsWith("@", StringComparison.Ordinal)));
                    for (int i = metadata.Locals.Count - 1; i >= 0; i--)
                        if (referencedVariables.Contains(metadata.Locals[i].Name) && metadata.Locals[i].SourceVariable != null)
                            referencedVariables.Add(metadata.Locals[i].SourceVariable);
                    metadata.Parameters.RemoveAll(parameter => !referencedVariables.Contains(parameter.Name));
                    metadata.Locals.RemoveAll(local => !referencedVariables.Contains(local.Name));
                    metadata.ReadOnly = executable.All(statement => statement.StartsWith("SELECT ", StringComparison.OrdinalIgnoreCase));
                    sql = string.Join(Environment.NewLine, statements.Select(statement =>
                        statement.StartsWith("-- UNSUPPORTED:", StringComparison.Ordinal) ? statement : statement + ";"));
                    if (metadata.Warnings.Count > 0)
                    {
                        sql = "-- PARTIAL: unsupported statements were skipped; review before relying on the results." + Environment.NewLine + sql;
                        errors.Add($"{metadata.Source}: partial conversion; {string.Join(" | ", metadata.Warnings)}");
                    }
                }
                catch (Exception ex) when (ex is NotSupportedException || ex is SQLiteException)
                {
                    metadata.Error = ex.Message;
                    errors.Add($"{metadata.Source}: {ex.Message}");
                    sql = "-- UNSUPPORTED: " + ex.Message.Replace("\r", " ").Replace("\n", " ") + Environment.NewLine +
                        "-- Original SQL Server definition (not executable in SQLite):" + Environment.NewLine +
                        string.Join(Environment.NewLine, (proc.CreateSql ?? "").Replace("\r\n", "\n").Split('\n').Select(line => "-- " + line));
                }
                File.WriteAllText(path, Header + JsonSerializer.Serialize(metadata) + Environment.NewLine + sql, new UTF8Encoding(false));
            }
            File.WriteAllText(IndexPath(sqlitePath), JsonSerializer.Serialize(usedNames.OrderBy(name => name).ToArray()), new UTF8Encoding(false));
            return errors;
        }

        private static string CommentSkipped(string sql, string reason)
            => "-- UNSUPPORTED: " + reason.Replace("\r", " ").Replace("\n", " ") + Environment.NewLine +
                string.Join(Environment.NewLine, sql.Replace("\r\n", "\n").Replace('\r', '\n').Split('\n').Select(line => "-- " + line));

        private static List<string> ConvertProcedure(string definition, Metadata metadata, HashSet<string> objects, DbSchema schema)
        {
            if (string.IsNullOrWhiteSpace(definition))
                throw new NotSupportedException("Definition unavailable: permissions, encryption or CLR procedure.");
            var tokens = SQLiteViewMigration.Tokenize(definition);
            int position = 0;
            bool Take(string word)
            {
                if (position >= tokens.Count || !tokens[position].Equals(word, StringComparison.OrdinalIgnoreCase)) return false;
                position++;
                return true;
            }
            string Next() => position < tokens.Count ? tokens[position++] : throw new NotSupportedException("Incomplete procedure definition.");
            if (Take("CREATE"))
            {
                if (Take("OR") && !Take("ALTER")) throw new NotSupportedException("Unsupported procedure declaration.");
            }
            else if (!Take("ALTER")) throw new NotSupportedException("Expected CREATE or ALTER PROCEDURE.");
            if (!Take("PROCEDURE") && !Take("PROC")) throw new NotSupportedException("Expected PROCEDURE.");
            Next();
            if (Take(".")) Next();
            while (position < tokens.Count && tokens[position].StartsWith("@", StringComparison.Ordinal))
            {
                var parameter = new Parameter { Name = Next() };
                Take("AS");
                parameter.Type = ParameterType(Next());
                if (Take("("))
                {
                    while (position < tokens.Count && tokens[position] != ")")
                    {
                        string size = Next();
                        if (size != "," && !size.Equals("MAX", StringComparison.OrdinalIgnoreCase) && !int.TryParse(size, out _))
                            throw new NotSupportedException("Unsupported parameter type specification.");
                    }
                    if (!Take(")")) throw new NotSupportedException("Invalid parameter type.");
                }
                if (Take("="))
                {
                    parameter.HasDefault = true;
                    string value = Next();
                    if (value == "-" || value == "+") value += Next();
                    if (value.Equals("NULL", StringComparison.OrdinalIgnoreCase)) parameter.DefaultValue = null;
                    else if (value.StartsWith("'") && value.EndsWith("'"))
                        parameter.DefaultValue = value.Substring(1, value.Length - 2).Replace("''", "'");
                    else if (decimal.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out _))
                        parameter.DefaultValue = value;
                    else throw new NotSupportedException("Only literal parameter defaults are supported.");
                }
                if (metadata.Parameters.Any(item => item.Name.Equals(parameter.Name, StringComparison.OrdinalIgnoreCase)))
                    throw new NotSupportedException("Duplicate parameter.");
                if (parameter.HasDefault)
                {
                    try { DefaultValue(parameter); }
                    catch (Exception ex) when (ex is FormatException || ex is OverflowException)
                    {
                        throw new NotSupportedException($"Unsupported default value for {parameter.Name}.", ex);
                    }
                }
                if (Take("OUTPUT") || Take("OUT"))
                    metadata.Warnings.Add($"Output parameter {parameter.Name} is unavailable; statements using it are skipped.");
                else metadata.Parameters.Add(parameter);
                if (!Take(",")) break;
            }
            if (!Take("AS")) throw new NotSupportedException("Output/table parameters and procedure options are not supported.");
            var body = tokens.Skip(position).ToList();
            while (body.LastOrDefault() == ";") body.RemoveAt(body.Count - 1);
            if (body.FirstOrDefault()?.Equals("BEGIN", StringComparison.OrdinalIgnoreCase) == true)
            {
                if (!body.Last().Equals("END", StringComparison.OrdinalIgnoreCase))
                    throw new NotSupportedException("Unsupported procedure block.");
                body.RemoveAt(body.Count - 1);
                body.RemoveAt(0);
            }
            if (body.Count >= 3 && string.Join(" ", body.Take(3)).Equals("SET NOCOUNT ON", StringComparison.OrdinalIgnoreCase))
            {
                body.RemoveRange(0, 3);
                if (body.FirstOrDefault() == ";") body.RemoveAt(0);
            }
            var result = new List<string>();
            // Try the existing parser first to retain declarations without semicolons.
            var localBody = new List<string>(body);
            try
            {
                ReadLocals(localBody, metadata);
                if (localBody.Count > 0 && new[] { "+", "-", "*", "/", "%" }.Contains(localBody[0]))
                    throw new NotSupportedException("Unsupported local initializer expression.");
                body = localBody;
            }
            catch (NotSupportedException)
            {
                metadata.Locals.Clear();
            }
            var unavailable = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            ReadConditionalAssignments(body, metadata);
            body = SeparateStatements(body);
            metadata.ReadOnly = true;
            var statement = new List<string>();
            for (int tokenIndex = 0; tokenIndex <= body.Count; tokenIndex++)
            {
                string token = tokenIndex == body.Count ? ";" : body[tokenIndex];
                if (token != ";") { statement.Add(token); continue; }
                if (statement.Count == 0) continue;
                string original = string.Join(" ", statement);
                string verb = statement[0].ToUpperInvariant();
                bool assignment = verb == "SELECT" && statement.Select((item, index) => (item, index))
                    .TakeWhile(pair => !pair.item.Equals("FROM", StringComparison.OrdinalIgnoreCase) &&
                        !pair.item.Equals("WHERE", StringComparison.OrdinalIgnoreCase))
                    .Any(pair => pair.item.StartsWith("@") && pair.index + 1 < statement.Count && statement[pair.index + 1] == "=");
                if (new[] { "IF", "WHILE", "ELSE", "BEGIN", "END", "RETURN", "GOTO", "THROW", "RAISERROR" }.Contains(verb))
                {
                    string reason = "Control flow, declarations or assignments require manual conversion; remaining statements were skipped.";
                    metadata.Warnings.Add(reason);
                    result.Add(CommentSkipped(original + "; " + string.Join(" ", body.Skip(tokenIndex + 1)), reason));
                    break;
                }
                try
                {
                    if (verb == "DECLARE")
                    {
                        var locals = new Metadata { Parameters = metadata.Parameters, Locals = new List<Parameter>(metadata.Locals) };
                        var declaration = new List<string>(statement);
                        ReadLocals(declaration, locals);
                        if (declaration.Count != 0) throw new NotSupportedException("Unsupported local initializer expression.");
                        metadata.Locals = locals.Locals;
                        statement = new List<string>();
                        continue;
                    }
                    if (assignment || verb == "SET")
                        throw new NotSupportedException("Variable assignment or SET option is not supported.");
                    if (verb != "SELECT" && verb != "INSERT" && verb != "UPDATE" && verb != "DELETE")
                        throw new NotSupportedException("Only SELECT/INSERT/UPDATE/DELETE are supported; separate statements with semicolons.");
                    if (verb == "SELECT" && statement.Any(item => item.Equals("INTO", StringComparison.OrdinalIgnoreCase)))
                        throw new NotSupportedException("SELECT INTO is not supported.");
                    for (int i = 0; i < statement.Count; i++)
                    {
                        if (!statement[i].StartsWith("@", StringComparison.Ordinal)) continue;
                        if (unavailable.Contains(statement[i]))
                            throw new NotSupportedException($"Variable depends on unsupported SQL: {statement[i]}");
                        var parameter = metadata.Parameters.Concat(metadata.Locals)
                            .FirstOrDefault(item => item.Name.Equals(statement[i], StringComparison.OrdinalIgnoreCase));
                        if (parameter == null) throw new NotSupportedException($"Undeclared or system variable: {statement[i]}");
                        statement[i] = parameter.Name;
                    }

                    var numericVariables = new HashSet<string>(metadata.Parameters.Concat(metadata.Locals)
                        .Where(parameter => parameter.Type == DbType.Int64 || parameter.Type == DbType.Decimal || parameter.Type == DbType.Double)
                        .Select(parameter => parameter.Name), StringComparer.OrdinalIgnoreCase);
                    AddNumericUpdateColumns(statement, schema, numericVariables);
                    result.Add(SQLiteViewMigration.ConvertQuery(statement, objects, allowWrites: true, numericVariables));
                }
                catch (NotSupportedException ex)
                {
                    if (verb == "DECLARE" || verb == "SET" || assignment)
                        foreach (string variable in statement.Where(item => item.StartsWith("@", StringComparison.Ordinal)))
                            unavailable.Add(variable);
                    metadata.Warnings.Add(ex.Message);
                    result.Add(CommentSkipped(original, ex.Message));
                }
                statement = new List<string>();
            }
            if (result.Count == 0) throw new NotSupportedException("Procedure has no supported SQL statements.");
            return result;
        }

        private static void AddNumericUpdateColumns(List<string> statement, DbSchema schema, HashSet<string> numeric)
        {
            if (statement.Count < 4 || !statement[0].Equals("UPDATE", StringComparison.OrdinalIgnoreCase) ||
                statement.Any(token => new[] { "FROM", "JOIN", "SELECT" }.Contains(token, StringComparer.OrdinalIgnoreCase))) return;
            string Unquote(string token) => token.StartsWith("[") ? token.Substring(1, token.Length - 2).Replace("]]", "]") :
                token.StartsWith("\"") ? token.Substring(1, token.Length - 2).Replace("\"\"", "\"") : token;
            bool qualified = statement.Count > 4 && statement[2] == ".";
            int end = qualified ? 4 : 2;
            if (!statement[end].Equals("SET", StringComparison.OrdinalIgnoreCase)) return;
            string tableName = Unquote(statement[qualified ? 3 : 1]);
            var tables = schema.DbTables.Where(table => table.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase) &&
                (!qualified || table.TableSchemaName.Equals(Unquote(statement[1]), StringComparison.OrdinalIgnoreCase))).ToList();
            if (tables.Count != 1) return;
            var columns = new HashSet<string>(tables[0].Columns.Where(column => new[]
                { "tinyint", "smallint", "int", "bigint", "decimal", "numeric", "money", "smallmoney", "float", "real" }
                .Contains(column.Type?.Split('(')[0].Trim(), StringComparer.OrdinalIgnoreCase)).Select(column => column.Name), StringComparer.OrdinalIgnoreCase);
            for (int i = end + 1; i < statement.Count; i++)
                if (!statement[i].StartsWith("@") && !statement[i].StartsWith("'") &&
                    (i == 0 || statement[i - 1] != ".") && (i + 1 == statement.Count || statement[i + 1] != ".") && columns.Contains(Unquote(statement[i])))
                    numeric.Add(statement[i]);
        }

        private static void ReadConditionalAssignments(List<string> body, Metadata metadata)
        {
            const string atom = @"(?:@[\p{L}_][\p{L}\p{N}_]*|[+-]?\s*\d+)";
            var pattern = new Regex(@"\AIF\s+(?<left>" + atom + @")\s*(?<op><>|!=|<=|>=|=|<|>)\s*(?<right>" + atom +
                @")\s+SET\s+(?<target>@[\p{L}_][\p{L}\p{N}_]*)\s*=\s*(?<value>" + atom + @"(?:\s*[+-]\s*" + atom + @")*)",
                RegexOptions.IgnoreCase);
            int consumed = 0;
            var assignments = new List<ConditionalAssignment>();
            while (consumed < body.Count)
            {
                while (consumed < body.Count && body[consumed] == ";") consumed++;
                if (consumed == body.Count || !body[consumed].Equals("IF", StringComparison.OrdinalIgnoreCase)) break;
                var match = pattern.Match(string.Join(" ", body.Skip(consumed)));
                if (!match.Success) return;
                var assignment = new ConditionalAssignment
                {
                    Left = match.Groups["left"].Value, Operator = match.Groups["op"].Value,
                    Right = match.Groups["right"].Value, Target = match.Groups["target"].Value,
                    Expression = match.Groups["value"].Value
                };
                if (!metadata.Locals.Any(local => local.Type == DbType.Int64 && local.Name.Equals(assignment.Target, StringComparison.OrdinalIgnoreCase))) return;
                var variables = SQLiteViewMigration.Tokenize(match.Value).Where(token => token.StartsWith("@", StringComparison.Ordinal));
                if (variables.Any(variable => !metadata.Parameters.Concat(metadata.Locals)
                    .Any(parameter => parameter.Type == DbType.Int64 && parameter.Name.Equals(variable, StringComparison.OrdinalIgnoreCase)))) return;
                consumed += SQLiteViewMigration.Tokenize(match.Value).Count;
                assignments.Add(assignment);
            }
            // Do not detach an ELSE, block or unsupported expression from its original condition.
            if (assignments.Count == 0 || (consumed < body.Count && !new[] { "SELECT", "INSERT", "UPDATE", "DELETE" }
                .Contains(body[consumed], StringComparer.OrdinalIgnoreCase))) return;
            metadata.Assignments.AddRange(assignments);
            body.RemoveRange(0, consumed);
        }

        private static List<string> SeparateStatements(List<string> tokens)
        {
            var result = new List<string>();
            string verb = null;
            int depth = 0;
            bool insertSelect = false;
            bool controlFlow = false;
            foreach (string token in tokens)
            {
                string keyword = token.ToUpperInvariant();
                if (depth == 0 && new[] { "IF", "WHILE", "BEGIN", "END", "ELSE", "RETURN", "GOTO", "THROW", "RAISERROR" }.Contains(keyword))
                    controlFlow = true;
                bool start = new[] { "SELECT", "INSERT", "UPDATE", "DELETE", "DECLARE", "EXEC", "EXECUTE" }.Contains(keyword);
                bool compound = result.LastOrDefault()?.ToUpperInvariant() is "UNION" or "ALL" or "EXCEPT" or "INTERSECT";
                bool selectSource = keyword == "SELECT" && verb == "INSERT" && !insertSelect;
                if (!controlFlow && depth == 0 && start && verb != null && !compound && !selectSource)
                {
                    result.Add(";");
                    verb = null;
                }
                if (token == ";") { verb = null; insertSelect = false; }
                else if (verb == null) { verb = keyword; insertSelect = false; }
                else if (selectSource) insertSelect = true;
                result.Add(token);
                if (token == "(") depth++;
                if (token == ")") depth--;
            }
            return result;
        }

        private static void ReadLocals(List<string> body, Metadata metadata)
        {
            int position = 0;
            bool Take(string word)
            {
                if (position >= body.Count || !body[position].Equals(word, StringComparison.OrdinalIgnoreCase)) return false;
                position++;
                return true;
            }
            string Next() => position < body.Count ? body[position++] : throw new NotSupportedException("Incomplete DECLARE.");
            while (true)
            {
                while (Take(";")) { }
                if (!Take("DECLARE")) break;
                do
                {
                    string name = Next();
                    if (!Regex.IsMatch(name, @"^@[\p{L}_][\p{L}\p{N}_]*$") ||
                        metadata.Parameters.Concat(metadata.Locals).Any(item => item.Name.Equals(name, StringComparison.OrdinalIgnoreCase)))
                        throw new NotSupportedException($"Invalid or duplicate local variable: {name}");
                    Take("AS");
                    string type = Next();
                    var local = new Parameter { Name = name, Type = ParameterType(type), HasDefault = true };
                    if (Take("("))
                    {
                        while (position < body.Count && body[position] != ")")
                        {
                            string size = Next();
                            if (size != "," && !size.Equals("MAX", StringComparison.OrdinalIgnoreCase) && !int.TryParse(size, out _))
                                throw new NotSupportedException("Unsupported local variable type.");
                        }
                        if (!Take(")")) throw new NotSupportedException("Invalid local variable type.");
                    }
                    if (Take("="))
                    {
                        string value = Next();
                        string function = value.ToUpperInvariant();
                        if (function == "GETDATE" || function == "YEAR" || function == "MONTH" || function == "DAY")
                        {
                            if (!Take("(") || (function != "GETDATE" && (!Take("GETDATE") || !Take("(") || !Take(")"))) || !Take(")"))
                                throw new NotSupportedException("Only GETDATE() and YEAR/MONTH/DAY(GETDATE()) initializers are supported.");
                            if (function == "GETDATE" ? !type.Equals("datetime", StringComparison.OrdinalIgnoreCase) &&
                                !type.Equals("datetime2", StringComparison.OrdinalIgnoreCase) : !type.Equals("int", StringComparison.OrdinalIgnoreCase) &&
                                !type.Equals("bigint", StringComparison.OrdinalIgnoreCase))
                                throw new NotSupportedException("Date initializers require datetime/datetime2; date parts require int/bigint.");
                            local.Initializer = function;
                        }
                        else if (value.StartsWith("@", StringComparison.Ordinal))
                        {
                            var source = metadata.Parameters.Concat(metadata.Locals).FirstOrDefault(parameter =>
                                parameter.Name.Equals(value, StringComparison.OrdinalIgnoreCase));
                            if (source == null || !IsNumeric(source.Type) || source.Type != local.Type)
                                throw new NotSupportedException("Local initializer requires a previously declared numeric variable of the same type.");
                            local.SourceVariable = source.Name;
                        }
                        else
                        {
                            if (value == "-" || value == "+") value += Next();
                            if (value.Equals("NULL", StringComparison.OrdinalIgnoreCase)) local.DefaultValue = null;
                            else if (value.StartsWith("'") && value.EndsWith("'"))
                                local.DefaultValue = value.Substring(1, value.Length - 2).Replace("''", "'");
                            else if (decimal.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out _))
                                local.DefaultValue = value;
                            else throw new NotSupportedException("Local initializers must be literals or supported date functions.");
                            try { DefaultValue(local); }
                            catch (Exception ex) when (ex is FormatException || ex is OverflowException)
                            {
                                throw new NotSupportedException($"Unsupported initializer for {name}.", ex);
                            }
                        }
                    }
                    while (position < body.Count && (body[position] == "+" || body[position] == "-"))
                    {
                        if (!IsNumeric(local.Type)) throw new NotSupportedException("Initializer arithmetic requires a numeric type.");
                        string operation = Next();
                        string operand = Next();
                        if (!long.TryParse(operand, NumberStyles.None, CultureInfo.InvariantCulture, out long amount))
                            throw new NotSupportedException("Initializer offsets must be integer constants.");
                        try { local.Offset = checked(local.Offset + (operation == "-" ? -amount : amount)); }
                        catch (OverflowException ex) { throw new NotSupportedException("Initializer offset overflow.", ex); }
                    }
                    metadata.Locals.Add(local);
                } while (Take(","));
            }
            body.RemoveRange(0, position);
        }

        internal static Script Load(string connectionString, string name)
        {
            if (string.IsNullOrWhiteSpace(name) || name != Path.GetFileName(name) || name.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
                throw new ArgumentException("Invalid procedure script name.", nameof(name));
            string database = new SQLiteConnectionStringBuilder(connectionString).DataSource;
            if (!ActiveNames(database).Contains(name, StringComparer.OrdinalIgnoreCase))
                throw new FileNotFoundException($"Procedure is not in this database's current export: {name}");
            var script = Read(Path.Combine(Folder(database), name + ".txt"));
            if (!string.Equals(script.Metadata.DatabaseFile, Path.GetFileName(database), StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("Procedure script belongs to a different database.");
            if (script.Metadata.Error != null) throw new NotSupportedException(script.Metadata.Error);
            return script;
        }

        internal static List<Proc> List(string connectionString)
        {
            string database = new SQLiteConnectionStringBuilder(connectionString).DataSource;
            if (string.IsNullOrEmpty(database) || database == ":memory:") return new List<Proc>();
            string folder = Folder(database);
            if (!Directory.Exists(folder)) return new List<Proc>();
            var result = new List<Proc>();
            foreach (string name in ActiveNames(database))
            {
                string path = Path.Combine(folder, name + ".txt");
                using var reader = File.OpenText(path);
                string firstLine = reader.ReadLine();
                if (firstLine == null || !firstLine.StartsWith(Header, StringComparison.Ordinal)) continue;
                var metadata = JsonSerializer.Deserialize<Metadata>(firstLine.Substring(Header.Length));
                if (metadata.Error == null && string.Equals(metadata.DatabaseFile, Path.GetFileName(database), StringComparison.OrdinalIgnoreCase))
                    result.Add(new Proc { Name = Path.GetFileNameWithoutExtension(path), ScriptWarning = string.Join(" | ", metadata.Warnings) });
            }
            return result;
        }

        internal static IDbDataParameter[] Bind(Script script, IDataParameter[] supplied, DateTime? executionTime = null)
        {
            // All date-derived locals share one snapshot per invocation, including across result sets.
            DateTime now = executionTime ?? DateTime.Now;
            var values = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            foreach (var parameter in supplied ?? Array.Empty<IDataParameter>())
            {
                string name = "@" + parameter.ParameterName.TrimStart('@');
                if (parameter.Direction != ParameterDirection.Input ||
                    !script.Metadata.Parameters.Any(item => item.Name.Equals(name, StringComparison.OrdinalIgnoreCase)))
                    throw new ArgumentException($"Unknown or non-input parameter: {name}");
                if (!values.TryAdd(name, parameter.Value ?? DBNull.Value))
                    throw new ArgumentException($"Duplicate parameter: {name}");
            }
            var bound = script.Metadata.Parameters.Select(parameter =>
            {
                if (!values.TryGetValue(parameter.Name, out var value))
                {
                    if (!parameter.HasDefault) throw new ArgumentException($"Required parameter: {parameter.Name}");
                    value = DefaultValue(parameter);
                }
                return (IDbDataParameter)new SQLiteParameter(parameter.Name, parameter.Type) { Value = value };
            }).ToList();
            var boundValues = bound.ToDictionary(parameter => parameter.ParameterName, StringComparer.OrdinalIgnoreCase);
            foreach (var local in script.Metadata.Locals)
            {
                object value = local.SourceVariable != null ? boundValues[local.SourceVariable].Value : local.Initializer switch
                    {
                        null => DefaultValue(local),
                        "GETDATE" => now,
                        "YEAR" => (long)now.Year,
                        "MONTH" => (long)now.Month,
                        "DAY" => (long)now.Day,
                        _ => throw new NotSupportedException($"Unsupported local initializer: {local.Initializer}")
                    };
                if (value != DBNull.Value && value != null && local.Offset != 0)
                    value = local.Type switch
                    {
                        DbType.Int64 => (object)checked(Convert.ToInt64(value, CultureInfo.InvariantCulture) + local.Offset),
                        DbType.Decimal => checked(Convert.ToDecimal(value, CultureInfo.InvariantCulture) + local.Offset),
                        DbType.Double => Convert.ToDouble(value, CultureInfo.InvariantCulture) + local.Offset,
                        _ => throw new NotSupportedException("Initializer arithmetic requires a numeric type.")
                    };
                var parameter = new SQLiteParameter(local.Name, local.Type) { Value = value ?? DBNull.Value };
                bound.Add(parameter);
                boundValues.Add(local.Name, parameter);
            }
            long? Evaluate(string expression)
            {
                var tokens = SQLiteViewMigration.Tokenize(expression);
                long total = 0;
                int sign = 1;
                foreach (string token in tokens)
                {
                    if (token == "+") continue;
                    if (token == "-") { sign = -sign; continue; }
                    object value = token.StartsWith("@", StringComparison.Ordinal) ? boundValues[token].Value : token;
                    if (value == null || value == DBNull.Value) return null;
                    total = checked(total + checked(sign * Convert.ToInt64(value, CultureInfo.InvariantCulture)));
                    sign = 1;
                }
                return total;
            }
            foreach (var assignment in script.Metadata.Assignments)
            {
                long? left = Evaluate(assignment.Left), right = Evaluate(assignment.Right);
                if (!left.HasValue || !right.HasValue) continue;
                bool matches = assignment.Operator switch
                {
                    "=" => left == right, "<>" or "!=" => left != right,
                    "<" => left < right, ">" => left > right, "<=" => left <= right, ">=" => left >= right,
                    _ => throw new NotSupportedException("Unsupported conditional comparison.")
                };
                if (matches) boundValues[assignment.Target].Value = (object)Evaluate(assignment.Expression) ?? DBNull.Value;
            }
            return bound.ToArray();
        }

        private static Script Read(string path)
        {
            using var reader = File.OpenText(path);
            string header = reader.ReadLine();
            if (header == null || !header.StartsWith(Header, StringComparison.Ordinal))
                throw new IOException($"Not a DataPie procedure script: {path}");
            return new Script { Metadata = JsonSerializer.Deserialize<Metadata>(header.Substring(Header.Length)), Sql = reader.ReadToEnd() };
        }

        private static string Folder(string database)
            => Path.Combine(Path.GetDirectoryName(Path.GetFullPath(database)), "StoredProcedures");

        private static string IndexPath(string database)
            => Path.Combine(Folder(database), ".datapie-" + Hash(Path.GetFileName(database).ToUpperInvariant()) + ".json");

        private static string[] ActiveNames(string database)
        {
            string path = IndexPath(database);
            if (!File.Exists(path)) return Array.Empty<string>();
            var names = JsonSerializer.Deserialize<string[]>(File.ReadAllText(path));
            if (names == null || names.Any(name => string.IsNullOrWhiteSpace(name) ||
                name != Path.GetFileName(name) || name.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0))
                throw new IOException("Invalid procedure export index.");
            return names;
        }

        private static bool IsNumeric(DbType type) => type == DbType.Int64 || type == DbType.Decimal || type == DbType.Double;

        private static object DefaultValue(Parameter parameter)
        {
            string value = parameter.DefaultValue;
            if (value == null) return DBNull.Value;
            return parameter.Type switch
            {
                DbType.Int64 => long.Parse(value, CultureInfo.InvariantCulture),
                DbType.Decimal => decimal.Parse(value, CultureInfo.InvariantCulture),
                DbType.Double => double.Parse(value, CultureInfo.InvariantCulture),
                DbType.Boolean => decimal.Parse(value, CultureInfo.InvariantCulture) != 0,
                DbType.DateTime => DateTime.Parse(value, CultureInfo.InvariantCulture),
                DbType.Guid => Guid.Parse(value),
                _ => value
            };
        }

        private static string SafeName(string name)
        {
            string safe = new string(name.Select(ch => Array.IndexOf(Path.GetInvalidFileNameChars(), ch) >= 0 ? '_' : ch).ToArray()).TrimEnd('.', ' ');
            if (safe != name || Regex.IsMatch(safe, @"^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(\.|$)", RegexOptions.IgnoreCase))
                safe = "_" + safe + "_" + Hash(name);
            return safe;
        }
        private static string Hash(string value) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).Substring(0, 12);

        private static DbType ParameterType(string type) => type.ToLowerInvariant() switch
        {
            "int" or "smallint" or "tinyint" or "bigint" => DbType.Int64,
            "decimal" or "numeric" or "money" or "smallmoney" => DbType.Decimal,
            "float" or "real" => DbType.Double,
            "bit" => DbType.Boolean,
            "varchar" or "nvarchar" or "char" or "nchar" or "text" or "ntext" => DbType.String,
            "date" or "datetime" or "datetime2" or "smalldatetime" => DbType.DateTime,
            "uniqueidentifier" => DbType.Guid,
            "binary" or "varbinary" => DbType.Binary,
            _ => throw new NotSupportedException($"Unsupported parameter type: {type}")
        };
    }
}
