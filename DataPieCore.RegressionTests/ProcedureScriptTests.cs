using System.Data;
using System.Data.SQLite;
using System.Reflection;
using DataPieCore;
using DBUtil;

internal static class ProcedureScriptTests
{
    public static void Run(string root)
    {
        CheckPivotConversion();
        string directory = Path.Combine(root, "procedure-tests");
        Directory.CreateDirectory(directory);
        string path = Path.Combine(directory, "data.db");
        var schema = new DbSchema();
        schema.DbTables.Add(new TableStruct
        {
            Name = "items", TableSchemaName = "dbo", PrimaryKey = "id",
            Columns = new List<Column>
            {
                new Column { Name = "id", Type = "int", IsPrimaryKey = true, Default = "" },
                new Column { Name = "code", Type = "nvarchar", MaxLength = 100, IsNullable = true, Default = "" }
            }
        });
        void Add(string name, string body, string parameters = "", string owner = "dbo")
            => schema.DbProcs.Add(new Proc { Name = name, SchemaName = owner,
                CreateSql = $"CREATE PROCEDURE [{owner}].[{name}] {parameters} AS BEGIN SET NOCOUNT ON; {body} END" });
        foreach (string name in new[] { "SellOut", "ProductTypeModelMap" })
            schema.DbTables.Add(new TableStruct
            {
                Name = name, TableSchemaName = "dbo", PrimaryKey = "Model",
                Columns = new List<Column>
                {
                    new Column { Name = "Model", Type = "nvarchar", MaxLength = 50, IsPrimaryKey = true, Default = "" },
                    new Column { Name = "ProductType", Type = "nvarchar", MaxLength = 50, IsNullable = true, Default = "" }
                }
            });
        string updateFrom = "UPDATE [dbo].[SellOut] SET [ProductType] = a.ProductType " +
            "from [dbo].[ProductTypeModelMap] a WHERE SellOut.Model=a.Model";
        Add("UpdateSellOut", updateFrom);
        Add("UpdateSellOutWithLocals", "declare @year int=YEAR(GETDATE()) declare @month int=MONTH(GETDATE()) " + updateFrom);
        Add("UpdateSellOutAfterBadLocal", "declare @year int=YEAR(GETDATE()) - 1 " + updateFrom);
        Add("GetItems", "SELECT TOP (10) id, code FROM dbo.items WHERE id >= @min AND code = @code ORDER BY id;",
            "@Min int = 1, @Code nvarchar(20) = N'EUR'");
        Add("Required", "SELECT * FROM dbo.items WHERE id = @id;", "@id int");
        Add("AddItem", "INSERT INTO dbo.items (id, code) VALUES (@id, @code);", "@id int, @code nvarchar(100)");
        Add("UpdateItem", "UPDATE dbo.items SET code = @code WHERE id = @id;", "@id int, @code nvarchar(100)");
        Add("DeleteItem", "DELETE FROM dbo.items WHERE id = @id;", "@id int");
        Add("Atomic", "INSERT INTO dbo.items (id, code) VALUES (900, N'first'); INSERT INTO dbo.items (id, code) VALUES (900, N'duplicate');");
        Add("MultiResult", "SELECT 1 AS id; SELECT 2 AS id;");
        Add("same", "SELECT 1 AS id;");
        Add("same", "SELECT 2 AS id;", owner: "sales");
        Add("中文/过程", "SELECT N'中文' AS text;");
        Add("UnsupportedIf", "IF @id > 0 SELECT @id;", "@id int");
        Add("UnsupportedOutput", "SELECT @id;", "@id int OUTPUT");
        Add("UnsupportedDynamic", "EXEC(N'SELECT 1');");
        Add("MissingColumn", "SELECT missing FROM dbo.items;");
        Add("Assignment", "SELECT @id = 2;", "@id int = 0");
        Add("Defaults", "SELECT @flag AS flag, @amount AS amount, @value AS value;", "@flag bit = 1, @amount decimal(10,2) = -1.25, @value nvarchar(20) = NULL");
        Add("DateLocals", "declare @year int = Year(getdate())\n declare @month int = MONTH(GETDATE())\n" +
            "DECLARE @day int = DAY(GETDATE()); DECLARE @now datetime = GETDATE();" +
            "SELECT @YEAR AS year, @month AS month, @day AS day, @now AS snapshot; SELECT @year AS year;");
        Add("ConditionalDateLocals", "DECLARE @year int=YEAR(GETDATE()) DECLARE @month int=MONTH(GETDATE()) DECLARE @day int=DAY(GETDATE()) " +
            "IF @day<15 SET @month=@month-1 IF @year=2027 SET @year=2026 IF @month=0 SET @month=12 " +
            "SELECT @year AS year, @month AS month;");
        Add("ConditionalElse", "DECLARE @month int=1; IF @month=1 SET @month=2 ELSE SET @month=3; SELECT @month;");
        Add("ConditionalNull", "DECLARE @value int=4; IF @input>0 SET @value=@value+2-1; SELECT @value;", "@input int=NULL");
        Add("ConstantLocals", "DECLARE @code nvarchar(20) = N'O''Brien', @amount decimal(10,2) = -1.25, @empty int; " +
            "SELECT @code AS code, @amount AS amount, @empty AS empty_value;");
        Add("CommentedLocal", "-- declare @year int = Year(getdate())\nSELECT 1 AS id;");
        Add("LocalSet", "DECLARE @year int = YEAR(GETDATE()); SET @year = 2000; SELECT @year;");
        Add("LocalSelectAssignment", "DECLARE @year int = YEAR(GETDATE()); SELECT @year = 2000;");
        Add("DuplicateLocal", "DECLARE @YEAR int = 2000; SELECT @year;", "@year int = 1999");
        Add("ComplexLocal", "DECLARE @year int = YEAR(GETDATE()) - 1; SELECT @year;");
        Add("LateLocal", "SELECT 1; DECLARE @year int = YEAR(GETDATE()); SELECT @year;");
        Add("PartialWrite", "INSERT INTO dbo.items VALUES (701, N'first'); UPDATE dbo.items SET code = N'wrong' WHERE id + 1 = 702; INSERT INTO dbo.items VALUES (702, N'last');");
        Add("PartialQuery", "SELECT 1; SELECT code COLLATE Latin1_General_CI_AS FROM dbo.items; SELECT missing FROM dbo.items; SELECT 2;");
        Add("PartialControl", "SELECT 1; IF 1 = 0 BEGIN DELETE FROM dbo.items; END; DELETE FROM dbo.items;");
        Add("PartialAssignment", "SELECT 1; SELECT @id = 2; DELETE FROM dbo.items WHERE id = @id;", "@id int = 1");
        Add("PartialMultiline", "SELECT 1; SELECT N'x;\nDELETE FROM items;\n' + N'y'; SELECT 2;");
        Add("RecoverLocal", "DECLARE @bad int = ABS(-1); SELECT @bad; SELECT 42 AS value;");
        Add("RecoverAssignment", "DECLARE @id int = 1; SET @id = 2; SELECT @id; SELECT 42 AS value;");
        Add("RecoverOutput", "SELECT @id; SELECT 42 AS value;", "@id int OUTPUT");
        Add("RecoverExpression", "DECLARE @year int = YEAR(GETDATE()) - 1; SELECT @year; SELECT 42 AS value;");
        Add("RecoverRequired", "SET @id = 2; SELECT @id; SELECT 42 AS value;", "@id int");
        Add("PayrollStyle", "DECLARE @year int = YEAR(GETDATE()) DECLARE @month int = MONTH(GETDATE()) " +
            "DELETE FROM dbo.items WHERE id = 801 " +
            "INSERT INTO dbo.items (id, code) SELECT 801, @year*100+@month " +
            "UPDATE dbo.items SET code = 'mapped' FROM (SELECT 801 AS id) a WHERE items.id = a.id " +
            "UPDATE dbo.items SET code = 'done' WHERE id = 801 SELECT @year*100+@month AS period");
        Add("NoSemicolonPartial", "SELECT 1 SELECT 'a' + 'b' SELECT 2");
        Add("InsertNullUnion", "INSERT INTO dbo.items (id, code) " +
            "SELECT 850, isnull(SUM(id), 0) FROM dbo.items WHERE id = -1 " +
            "UNION ALL SELECT 851, ISNULL(SUM(id), 0) FROM dbo.items WHERE id = 1 GROUP BY code HAVING SUM(id) > 0.00001 " +
            "UNION ALL SELECT 852, IsNull(SUM(id), 0) FROM dbo.items WHERE id = 1 GROUP BY code HAVING SUM(id) > 0.00001 " +
            "UPDATE dbo.items SET code = ISNULL(NULL, 'updated') WHERE id = 852");
        Add("NullFunctions", "SELECT ISNULL(NULL, 0) AS empty_value, ISNULL(7, 0) AS present_value, " +
            "ISNULL(NULL, ISNULL(NULL, 'fallback')) AS nested_value, 'isnull(NULL, 0)' AS [ISNULL], NULLIF(1, 1) AS null_value, COALESCE(NULL, 3) AS coalesced;");
        schema.DbProcs.Add(new Proc { SchemaName = "dbo", Name = "Encrypted" });

        SqlServerToSQLite.dbs = schema;
        var result = SqlServerToSQLite.CreateSQLiteDatabase(path, null, false);
        Check(result.ProcedureErrors.Count == 23, "Partial and unsupported procedures must be explicitly reported");
        int supportedCount = schema.DbProcs.Count - 11;
        string folder = Path.Combine(directory, "StoredProcedures");
        Check(Directory.GetFiles(folder, "*.txt").Length == schema.DbProcs.Count, "One text file per procedure");
        Check(File.ReadAllText(Path.Combine(folder, "UnsupportedIf.txt")).Contains("-- UNSUPPORTED:"), "Unsupported file marker");
        Check(File.ReadAllText(Path.Combine(folder, "GetItems.txt")).Contains("LIMIT 10"), "Reusable TOP conversion");
        Check(!File.ReadAllText(Path.Combine(folder, "GetItems.txt")).Contains("CREATE PROCEDURE"), "Export contains executable body, not T-SQL declaration");

        using var db = DbAccessFactory.Create($"Data Source={path};Pooling=False", "SQLITE");
        Check(db.GetProcs().Count == supportedCount, "Only supported scripts appear in the procedure list");
        Check(db.GetDataTable("SELECT * FROM items").Rows.Count == 0, "Export validation must not execute writes");
        db.ExecuteSql("INSERT INTO SellOut VALUES ('matched','old'),('unmatched','keep'); INSERT INTO ProductTypeModelMap VALUES ('matched','new');");
        foreach (string name in new[] { "UpdateSellOut", "UpdateSellOutWithLocals", "UpdateSellOutAfterBadLocal" })
        {
            db.ExecuteSql("UPDATE SellOut SET ProductType='old' WHERE Model='matched'");
            db.RunProcedure(name);
            var rows = db.GetDataTable("SELECT ProductType FROM SellOut ORDER BY Model");
            Check((string)rows.Rows[0][0] == "new" && (string)rows.Rows[1][0] == "keep",
                "UPDATE FROM preserves matching and survives independent unsupported declarations");
        }
        IDataParameter[] Values(int id, string code) => new IDataParameter[]
            { db.CreatePara("@id", id), db.CreatePara("@code", code) };
        Check(db.RunProcedure("AddItem", Values(1, "EUR"), out int affected) == 1 && affected == 1, "Parameterized insert");
        db.RunProcedure("AddItem", Values(2, "CH"), out _);
        using (var reader = db.RunProcedure("GetItems", Array.Empty<IDataParameter>()))
            Check(reader.Read() && reader.GetInt64(0) == 1 && !reader.Read(), "Default parameters and reader execution");
        Check(db.conn.State == ConnectionState.Closed, "Procedure reader releases its connection");
        using (var data = db.RunProcedure("GetItems", new IDataParameter[] { db.CreatePara("code", "CH") }, "items"))
            Check(data.Tables[0].Rows.Count == 1 && Convert.ToInt32(data.Tables[0].Rows[0]["id"]) == 2, "Named parameter override and DataSet execution");
        Throws<ArgumentException>(() => db.RunProcedure("Required"), "Missing required parameter");
        Throws<ArgumentException>(() => db.RunProcedure("Required", new IDataParameter[] { db.CreatePara("@bad", 1) }, out _), "Unknown parameter");
        Throws<NotSupportedException>(() => db.RunProcedure("UnsupportedIf"), "Unsupported scripts cannot execute");
        Throws<ArgumentException>(() => db.RunProcedure("../outside"), "Procedure names cannot escape script directory");
        string payload = "EUR'; DELETE FROM items; --";
        db.RunProcedure("UpdateItem", Values(2, payload), out _);
        using (var data = db.RunProcedure("GetItems", new IDataParameter[] { db.CreatePara("@code", payload) }, "items"))
            Check(data.Tables[0].Rows.Count == 1 && db.GetDataTable("SELECT * FROM items").Rows.Count == 2, "Values are bound, not interpolated");
        Throws<SQLiteException>(() => db.RunProcedure("Atomic"), "A failing write script must throw");
        Check(db.GetDataTable("SELECT * FROM items WHERE id = 900").Rows.Count == 0, "Write script rolls back all statements");
        using (var data = db.RunProcedure("MultiResult", Array.Empty<IDataParameter>(), "result"))
            Check(data.Tables.Count == 2 && Convert.ToInt32(data.Tables[1].Rows[0][0]) == 2, "Multiple query results");
        db.RunProcedure("DeleteItem", new IDataParameter[] { db.CreatePara("@id", 2) }, out _);
        Check(db.GetDataTable("SELECT * FROM items").Rows.Count == 1, "Parameterized delete");
        using (var reader = db.RunProcedure("sales.same", Array.Empty<IDataParameter>()))
            Check(reader.Read() && reader.GetInt64(0) == 2, "Same-name procedures use schema-qualified filenames");
        using (var data = db.RunProcedure("Defaults", Array.Empty<IDataParameter>(), "defaults"))
            Check(Convert.ToInt64(data.Tables[0].Rows[0]["flag"]) == 1 &&
                Convert.ToDecimal(data.Tables[0].Rows[0]["amount"]) == -1.25m &&
                data.Tables[0].Rows[0]["value"] == DBNull.Value, "Typed and null parameter defaults");
        using (var data = db.RunProcedure("DateLocals", Array.Empty<IDataParameter>(), "dates"))
        {
            var row = data.Tables[0].Rows[0];
            DateTime snapshot = Convert.ToDateTime(row["snapshot"]);
            Check(Convert.ToInt32(row["year"]) == snapshot.Year &&
                Convert.ToInt32(row["month"]) == snapshot.Month && Convert.ToInt32(row["day"]) == snapshot.Day &&
                Convert.ToInt32(data.Tables[1].Rows[0]["year"]) == snapshot.Year, "Date locals share one timestamp across statements");
        }
        using (var data = db.RunProcedure("ConstantLocals", Array.Empty<IDataParameter>(), "locals"))
            Check((string)data.Tables[0].Rows[0]["code"] == "O'Brien" &&
                Convert.ToDecimal(data.Tables[0].Rows[0]["amount"]) == -1.25m &&
                data.Tables[0].Rows[0]["empty_value"] == DBNull.Value, "Local constants, comma declarations and implicit NULL");
        Throws<ArgumentException>(() => db.RunProcedure("DateLocals", new IDataParameter[] { db.CreatePara("@year", 2000) }, out _),
            "Caller cannot override local variables");
        Throws<NotSupportedException>(() => db.RunProcedure("LocalSet"), "Reassignment is unsupported");
        string localBody = string.Join("\n", File.ReadAllLines(Path.Combine(folder, "DateLocals.txt")).Skip(1));
        Check(!localBody.Contains("DECLARE", StringComparison.OrdinalIgnoreCase) && localBody.Contains("@year"),
            "Export strips declarations while retaining parameter references");
        Check(File.ReadAllLines(Path.Combine(folder, "DateLocals.txt"))[0].Contains("\"Initializer\":\"YEAR\""),
            "Export stores date rules instead of today's year");
        Check(!string.IsNullOrEmpty(db.GetProcs().Single(proc => proc.Name == "PartialWrite").ScriptWarning), "Partial scripts expose warnings");
        db.RunProcedure("PartialWrite");
        Check(db.GetDataTable("SELECT * FROM items WHERE id IN (701, 702)").Rows.Count == 2, "Statements before and after unsupported SQL execute");
        Check(db.GetDataTable("SELECT * FROM items WHERE code = 'wrong'").Rows.Count == 0, "Unsupported WHERE skips the entire UPDATE");
        foreach (string name in new[] { "PartialQuery", "PartialMultiline" })
            using (var data = db.RunProcedure(name, Array.Empty<IDataParameter>(), "partial"))
                Check(data.Tables.Count == 2 && Convert.ToInt32(data.Tables[1].Rows[0][0]) == 2, "Compatible queries survive conversion and validation failures");
        db.RunProcedure("PartialControl");
        db.RunProcedure("PartialAssignment");
        Check(db.GetDataTable("SELECT * FROM items").Rows.Count == 3, "Control flow and assignment suffixes never execute");
        string partialSql = File.ReadAllText(Path.Combine(folder, "PartialWrite.txt"));
        Check(partialSql.Contains("-- PARTIAL:") && partialSql.Contains("-- UNSUPPORTED:") && partialSql.Contains("-- UPDATE"), "Skipped SQL remains commented for inspection");
        foreach (string name in new[] { "RecoverLocal", "RecoverAssignment", "RecoverOutput", "RecoverExpression", "RecoverRequired" })
        {
            using var data = db.RunProcedure(name, Array.Empty<IDataParameter>(), "recovered");
            Check(data.Tables.Count == 1 && Convert.ToInt32(data.Tables[0].Rows[0][0]) == 42,
                "Unsupported declarations and assignments preserve independent executable SQL");
            Check(File.ReadAllText(Path.Combine(folder, name + ".txt")).Contains("-- UNSUPPORTED:"),
                "Callable scripts retain unsupported statement markers");
        }
        // Exercise the same binder with deterministic timestamps on either side of a year boundary.
        var scripts = typeof(SqlServerToSQLite).Assembly.GetType("DataPieCore.SQLiteProcedureScripts")!;
        var load = scripts.GetMethod("Load", BindingFlags.Static | BindingFlags.NonPublic)!;
        var bind = scripts.GetMethod("Bind", BindingFlags.Static | BindingFlags.NonPublic)!;
        object script = load.Invoke(null, new object[] { db.ConnectionString, "DateLocals" })!;
        object conditional = load.Invoke(null, new object[] { db.ConnectionString, "ConditionalDateLocals" })!;
        foreach (var test in new[] { (new DateTime(2027,1,14),2026,12), (new DateTime(2027,1,15),2026,1),
            (new DateTime(2026,1,1),2026,12), (new DateTime(2028,3,14),2028,2), (new DateTime(2028,3,15),2028,3) })
        {
            var parameters = (IDbDataParameter[])bind.Invoke(null, new object[] { conditional, Array.Empty<IDataParameter>(), test.Item1 })!;
            var values = parameters.ToDictionary(parameter => parameter.ParameterName, parameter => parameter.Value);
            Check(Convert.ToInt32(values["@year"]) == test.Item2 && Convert.ToInt32(values["@month"]) == test.Item3,
                "Conditional assignments run sequentially per invocation and retain original year rules");
        }
        using (var data = db.RunProcedure("ConditionalNull", Array.Empty<IDataParameter>(), "result"))
            Check(Convert.ToInt32(data.Tables[0].Rows[0][0]) == 4, "NULL IF condition does not execute assignment");
        using (var data = db.RunProcedure("ConditionalNull", new IDataParameter[] { db.CreatePara("@input", 1) }, "result"))
            Check(Convert.ToInt32(data.Tables[0].Rows[0][0]) == 5, "Conditional arithmetic uses input values");
        Throws<NotSupportedException>(() => db.RunProcedure("ConditionalElse"), "Unsupported ELSE must not be detached from IF");
        foreach (var now in new[] { new DateTime(2024, 12, 31, 23, 59, 59), new DateTime(2025, 1, 1, 0, 0, 0) })
        {
            var bound = (IDbDataParameter[])bind.Invoke(null, new object[] { script, Array.Empty<IDataParameter>(), now })!;
            var values = bound.ToDictionary(parameter => parameter.ParameterName, parameter => parameter.Value);
            Check((long)values["@year"] == now.Year && (long)values["@month"] == now.Month &&
                (long)values["@day"] == now.Day && (DateTime)values["@now"] == now, "Locals are re-evaluated for each invocation");
        }
        File.Copy(Path.Combine(folder, "AddItem.txt"), Path.Combine(folder, "StaleProcedure.txt"));
        db.RunProcedure("PayrollStyle");
        db.RunProcedure("InsertNullUnion");
        var inserted = db.GetDataTable("SELECT code FROM items WHERE id BETWEEN 850 AND 852 ORDER BY id");
        Check(inserted.Rows.Count == 3 && Convert.ToString(inserted.Rows[0][0]) == "0" &&
            Convert.ToString(inserted.Rows[1][0]) == "1" && Convert.ToString(inserted.Rows[2][0]) == "updated",
            "INSERT SELECT UNION ALL supports ISNULL aggregates, GROUP BY/HAVING and subsequent UPDATE");
        using (var data = db.RunProcedure("NullFunctions", Array.Empty<IDataParameter>(), "nulls"))
        {
            var row = data.Tables[0].Rows[0];
            Check(Convert.ToInt32(row[0]) == 0 && Convert.ToInt32(row[1]) == 7 && (string)row[2] == "fallback" &&
                (string)row["ISNULL"] == "isnull(NULL, 0)" && row[4] == DBNull.Value && Convert.ToInt32(row[5]) == 3,
                "Null conversion preserves nested calls, literals, quoted identifiers and native null functions");
        }
        Check((string)db.GetDataTable("SELECT code FROM items WHERE id = 801").Rows[0][0] == "done",
            "Semicolon-free DELETE, INSERT SELECT and UPDATE FROM remain separate executable statements");
        using (var data = db.RunProcedure("NoSemicolonPartial", Array.Empty<IDataParameter>(), "result"))
            Check(data.Tables.Count == 2, "One unsupported expression does not suppress adjacent semicolon-free queries");
        Check(db.GetProcs().Count == supportedCount, "Unindexed scripts are not exposed");
        Throws<FileNotFoundException>(() => db.RunProcedure("StaleProcedure"), "Stale scripts cannot execute");
        Check(Directory.EnumerateFiles(directory, "*.txt").Count() == 0, "Unsafe filename stays inside StoredProcedures");
        Console.WriteLine("PASS: procedure scripts, parameters/defaults, CRUD, atomic rollback, result sets, filenames and unsupported markers.");
    }

    private static void CheckPivotConversion()
    {
        var converter = typeof(SqlServerToSQLite).Assembly.GetType("DataPieCore.SQLiteViewMigration")!;
        var tokenize = converter.GetMethod("Tokenize", BindingFlags.Static | BindingFlags.NonPublic)!;
        var convert = converter.GetMethod("ConvertQuery", BindingFlags.Static | BindingFlags.NonPublic)!;
        string ConvertSql(string sql)
        {
            var tokens = (List<string>)tokenize.Invoke(null, new object[] { sql })!;
            return (string)convert.Invoke(null, new object?[] { tokens,
                new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "dbo.source", "dbo.target" }, true, null })!;
        }
        using var connection = new SQLiteConnection("Data Source=:memory:");
        connection.Open();
        void Execute(string sql) { using var command = new SQLiteCommand(sql, connection); command.ExecuteNonQuery(); }
        Execute("CREATE TABLE source (customer TEXT, detail TEXT, metric TEXT, amount REAL, remark TEXT); " +
            "CREATE TABLE target (customer TEXT, revenue REAL, qty REAL); " +
            "INSERT INTO source VALUES ('A','x','Revenue',10,'USD'),('A','y','Revenue',20,'USD')," +
            "('A','x','Qty',2,'USD'),('B','x','Revenue',50,'USD'),('B','y','Qty',NULL,'USD')," +
            "('C','x','Revenue',100,'EUR'),('D','x','Other',9,'USD');");
        using (var command = new SQLiteCommand(ConvertSql("SELECT LEFT('2026-09',4), YEAR('2026-09-19 12:30:00')*100+MONTH('2026-09-19 12:30:00'), " +
            "SUBSTRING('ABC123  ',4,LEN('ABC123  ')-3), LEN('ABC  '), LEFT(ISNULL(NULL,'abcd'),2), " +
            "SUBSTRING('abcd',0,3), DAY('2024-02-29'), LEFT(NULL,4), YEAR(NULL), SUBSTRING('ABC',4,LEN('ABC')-3), 'LEFT(x,4)'"), connection))
        using (var reader = command.ExecuteReader())
        {
            Check(reader.Read() && reader.GetString(0) == "2026" && reader.GetInt64(1) == 202609 &&
                reader.GetString(2) == "123" && reader.GetInt64(3) == 3 && reader.GetString(4) == "ab" &&
                reader.GetString(5) == "ab" && reader.GetInt64(6) == 29 && reader.IsDBNull(7) && reader.IsDBNull(8) &&
                reader.GetString(9) == "" && reader.GetString(10) == "LEFT(x,4)", "Scalar conversions preserve dates, nested calls, trailing spaces, NULL and substring boundaries");
        }
        foreach (string invalid in new[] { "SELECT LEFT('abc',-1)", "SELECT SUBSTRING('AB',4,LEN('AB')-3)", "SELECT YEAR('not-a-date')" })
            Throws<SQLiteException>(() => Execute(ConvertSql(invalid)), "Invalid length/date must fail rather than silently alter values");
        Execute(ConvertSql("UPDATE dbo.source SET detail=LEFT(customer,1), amount=YEAR('2026-09-19')*100+MONTH('2026-09-19') WHERE customer='D'"));
        using (var command = new SQLiteCommand("SELECT detail,amount FROM source WHERE customer='D'", connection))
        using (var reader = command.ExecuteReader())
            Check(reader.Read() && reader.GetString(0) == "D" && reader.GetDouble(1) == 202609, "Scalar conversions execute in UPDATE assignments");
        Execute("UPDATE source SET detail='x',amount=9 WHERE customer='D'");
        string select = "SELECT TOP 1 customer, SUM(ISNULL([Revenue],0)) AS revenue, SUM(ISNULL([Qty],0)) AS qty " +
            "FROM dbo.source PIVOT (SUM(amount) FOR metric IN ([Revenue],[Qty])) AS PivotTable " +
            "WHERE remark='USD' GROUP BY customer ORDER BY revenue DESC";
        string converted = ConvertSql("INSERT INTO dbo.target (customer,revenue,qty) " + select);
        Check(converted.Contains("LIMIT 1") && !converted.Contains("PIVOT"), "INSERT TOP and PIVOT are converted");
        Execute(converted);
        using (var command = new SQLiteCommand("SELECT customer,revenue,qty FROM target", connection))
        using (var reader = command.ExecuteReader())
            Check(reader.Read() && reader.GetString(0) == "B" && reader.GetDouble(1) == 50 && reader.GetDouble(2) == 0 && !reader.Read(),
                "TOP selects highest aggregate and missing pivot values become zero");
        Execute("DELETE FROM target");
        Execute(ConvertSql("INSERT INTO dbo.target (customer,revenue,qty) " + select.Replace("TOP 1", "TOP (100000)")));
        using (var command = new SQLiteCommand("SELECT revenue,qty FROM target WHERE customer='A'", connection))
        using (var reader = command.ExecuteReader())
            Check(reader.Read() && reader.GetDouble(0) == 30 && reader.GetDouble(1) == 2, "Pivot sums across implicit source groups");
        using (var command = new SQLiteCommand("SELECT revenue,qty FROM target WHERE customer='D'", connection))
        using (var reader = command.ExecuteReader())
            Check(reader.Read() && reader.GetDouble(0) == 0 && reader.GetDouble(1) == 0, "Unmatched pivot keys preserve zero-valued groups");
        Execute("DELETE FROM target");
        Execute(ConvertSql("INSERT INTO dbo.target (customer,revenue,qty) SELECT TOP (1) customer,amount,0 FROM dbo.source ORDER BY amount DESC"));
        using (var command = new SQLiteCommand("SELECT customer FROM target", connection))
            Check((string)command.ExecuteScalar()! == "C", "INSERT TOP also works without PIVOT");
        Execute("INSERT INTO target VALUES ('keep',1,1)");
        Execute(ConvertSql("delete [dbo].[target] WHERE customer = 'C'"));
        using (var command = new SQLiteCommand("SELECT customer FROM target", connection))
        using (var reader = command.ExecuteReader())
            Check(reader.Read() && reader.GetString(0) == "keep" && !reader.Read(), "DELETE without FROM preserves its filter");
        Check(ConvertSql("delete [dbo].[target]") == "delete FROM \"target\"", "DELETE schema target gains FROM and maps the schema");
        Execute(ConvertSql("delete [dbo].[target]"));
        Execute("INSERT INTO target VALUES ('again',1,1)");
        Execute(ConvertSql("DELETE target WHERE customer = 'again'"));
        using (var command = new SQLiteCommand("SELECT COUNT(*) FROM target", connection))
            Check(Convert.ToInt64(command.ExecuteScalar()) == 0, "Qualified and unqualified DELETE without FROM execute");
        Check(ConvertSql("DELETE FROM dbo.target WHERE customer = 'keep'").Contains("WHERE customer = 'keep'"),
            "Existing DELETE FROM syntax stays supported");
        foreach (string unsupported in new[] { select.Replace("TOP 1", "TOP 1 PERCENT"),
            select.Replace("TOP 1", "TOP 1 WITH TIES"), select.Replace("SUM(amount)", "AVG(amount)"),
            select.Replace("SUM(ISNULL([Qty],0))", "COUNT(*)"),
            select.Replace("remark='USD'", "[Qty]>0"), select + " UNION ALL SELECT 1,2,3",
            "DELETE t FROM dbo.target t JOIN dbo.source s ON t.customer=s.customer",
            "DELETE TOP (1) dbo.target", "DELETE other.dbo.target", "DELETE missing.target" })
        {
            try { ConvertSql(unsupported); }
            catch (TargetInvocationException ex) when (ex.InnerException is NotSupportedException) { continue; }
            throw new Exception("Unsafe TOP/PIVOT shapes must be rejected");
        }
    }

    private static void Check(bool condition, string message)
    {
        if (!condition) throw new Exception(message);
    }
    private static void Throws<T>(Action action, string message) where T : Exception
    {
        try { action(); }
        catch (T) { return; }
        throw new Exception(message);
    }
}
