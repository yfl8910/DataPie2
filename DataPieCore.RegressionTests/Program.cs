using System.Data;
using System.Data.SQLite;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Text;
using DataPieCore;
using DBUtil;
using ExcelDataReader;

Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
string directory = Path.Combine(Path.GetTempPath(), "DataPie-regression-" + Guid.NewGuid().ToString("N"));
Directory.CreateDirectory(directory);
string connectionString = new SQLiteConnectionStringBuilder { DataSource = Path.Combine(directory, "test.db"), Pooling = false }.ToString();
IDbAccess Open() => DbAccessFactory.Create(connectionString, "SQLITE");
void Check(bool condition, string message) { if (!condition) throw new Exception(message); }
void Execute(string sql) { using var db = Open(); db.ExecuteSql(sql); }
long Count(string table) { using var db = Open(); return Convert.ToInt64(db.GetDataTable("SELECT COUNT(*) FROM " + table).Rows[0][0]); }

try
{
    SqlServerAccessTests.Run();
    SchemaMigrationTests.Run(directory);
    ProcedureScriptTests.Run(directory);
    using (var metadataDb = (SQLiteDbAccess)DbAccessFactory.Create($"Data Source={Path.Combine(directory, "metadata.db")};Pooling=False", "SQLITE"))
    {
        metadataDb.ExecuteSql("CREATE TABLE [odd table] (id INTEGER PRIMARY KEY AUTOINCREMENT, code VARCHAR(25) NOT NULL UNIQUE, note TEXT); CREATE VIEW [odd view] AS SELECT code FROM [odd table]");
        var table = metadataDb.ShowTables().Single(item => item.Name == "odd table");
        var columns = metadataDb.ShowColumns("odd table");
        Check(table.Columns.Count == 3 && table.Columns.Select(column => column.Name).SequenceEqual(columns.Select(column => column.Name)),
            "SQLite table listing retains ordered column metadata");
        Check(table.Columns[0].IsPrimaryKey && table.Columns[0].IsIdentity && table.Columns[1].IsUnique &&
            !table.Columns[1].IsNullable && table.Columns[1].MaxLength == 25 && table.Columns[2].IsNullable,
            "SQLite primary key, identity, uniqueness, length and nullability metadata are preserved");
        Check(metadataDb.ShowViews().SequenceEqual(new[] { "odd view" }) && metadataDb.conn.State == ConnectionState.Closed,
            "SQLite view reader returns names and releases its connection");
        Check(metadataDb.GetDataBaseInfo().Contains("main"), "SQLite catalogs remain available");
        bool schemaFailed = false;
        try { using var invalidSchema = metadataDb.GetSchema("NotARealCollection"); }
        catch (Exception ex) when (ex is not NullReferenceException) { schemaFailed = true; }
        Check(schemaFailed, "Schema errors propagate instead of returning null");
    }
    Console.WriteLine("PASS: SQLite metadata readers, column details, catalogs and schema error propagation.");
    var migrationSchema = new DbSchema();
    migrationSchema.Tables.Add(new TableStruct
    {
        TableSchemaName = "dbo", Name = "source",
        Columns = new List<Column> { new Column { Name = "id", Type = "int", IsNullable = true, Default = "" } }
    });
    migrationSchema.ViewDefinitions.AddRange(new[]
    {
        new ViewSchema { SchemaName = "dbo", ViewName = "dependent", ViewSQL = "CREATE VIEW dbo.dependent AS SELECT * FROM dbo.valid" },
        new ViewSchema
        {
            SchemaName = "dbo", ViewName = "unicode_filter",
            ViewSQL = "CREATE VIEW dbo.unicode_filter AS SELECT id, N'O''Brien' AS author, n'中文' AS label, " +
                "N'' AS empty_value, 'N''CH''' AS literal_text, N'-- /* dbo.source ;' AS markers, N'EUR' AS [N] " +
                "FROM dbo.source WHERE (CASE WHEN id = 42 THEN N'CH' ELSE N'USD' END) IN (N'CH', N'EUR')"
        },
        new ViewSchema { SchemaName = "dbo", ViewName = "valid", ViewSQL = "CREATE OR ALTER VIEW [dbo].[valid] (id, label) AS SELECT id, 'dbo.source; unchanged' FROM [dbo].[source];" },
        new ViewSchema { SchemaName = "dbo", ViewName = "brokenDependent", ViewSQL = "CREATE VIEW dbo.brokenDependent AS SELECT * FROM dbo.broken" },
        new ViewSchema { SchemaName = "dbo", ViewName = "broken", ViewSQL = "CREATE VIEW dbo.broken AS SELECT missing FROM dbo.source" },
        new ViewSchema { SchemaName = "dbo", ViewName = "unsupported", ViewSQL = "CREATE VIEW dbo.unsupported AS SELECT TOP 1 PERCENT id FROM dbo.source" },
        new ViewSchema { SchemaName = "dbo", ViewName = "top_group", ViewSQL = "CREATE VIEW dbo.top_group AS SELECT TOP (1) [Product Type], 客户 AS Customer, SUM(Qty) AS Qty FROM (SELECT id AS [Product Type], id AS 客户, id AS Qty, N'EUR' AS Remark FROM dbo.source) AS totals WHERE Remark = N'EUR' GROUP BY 客户, [Product Type] ORDER BY Qty DESC" },
        new ViewSchema { SchemaName = "dbo", ViewName = "top_plain", ViewSQL = "CREATE VIEW dbo.top_plain AS SELECT DISTINCT TOP 1 id FROM dbo.source ORDER BY id DESC" },
        new ViewSchema { SchemaName = "dbo", ViewName = "top_zero", ViewSQL = "CREATE VIEW dbo.top_zero AS SELECT TOP (0) id FROM dbo.source" },
        new ViewSchema { SchemaName = "dbo", ViewName = "top_ties", ViewSQL = "CREATE VIEW dbo.top_ties AS SELECT TOP (1) WITH TIES id FROM dbo.source ORDER BY id" },
        new ViewSchema { SchemaName = "dbo", ViewName = "top_union", ViewSQL = "CREATE VIEW dbo.top_union AS SELECT TOP (1) id FROM dbo.source UNION ALL SELECT id FROM dbo.source" },
        new ViewSchema { SchemaName = "dbo", ViewName = "unavailable", ViewSQL = null },
        new ViewSchema { SchemaName = "sales", ViewName = "source", ViewSQL = "CREATE VIEW sales.source AS SELECT 1 AS id" }
    });
    SqlServerToSQLite.dbs = migrationSchema;
    string migrationPath = Path.Combine(directory, "views.db");
    var viewErrors = SqlServerToSQLite.CreateSQLiteDatabase(migrationPath, null, true).ViewErrors;
    Check(viewErrors.Count == 7, "Invalid definitions, unsafe TOP variants, dependencies and name conflicts are reported");
    using (var db = DbAccessFactory.Create($"Data Source={migrationPath};Pooling=False", "SQLITE"))
    {
        Check(db.ShowViews().Count == 6, "Only validated views remain");
        db.ExecuteSql("INSERT INTO source VALUES (42)");
        var actual = db.GetDataTable("SELECT * FROM dependent");
        Check(actual.Rows.Count == 1 && Convert.ToInt32(actual.Rows[0]["id"]) == 42 &&
            (string)actual.Rows[0]["label"] == "dbo.source; unchanged", "Views are live and string literals are preserved");
        db.ExecuteSql("INSERT INTO source VALUES (43)");
        var topGroup = db.GetDataTable("SELECT * FROM top_group");
        Check(topGroup.Rows.Count == 1 && Convert.ToInt32(topGroup.Rows[0]["Qty"]) == 43 &&
            Convert.ToInt32(topGroup.Rows[0]["Customer"]) == 43, "TOP applies after grouping and descending aggregate order");
        Check(Convert.ToInt32(db.GetDataTable("SELECT * FROM top_plain").Rows[0]["id"]) == 43,
            "Unparenthesized TOP with DISTINCT");
        Check(db.GetDataTable("SELECT * FROM top_zero").Rows.Count == 0, "TOP zero returns no rows");
        var filtered = db.GetDataTable("SELECT * FROM unicode_filter WHERE [N] = 'EUR'");
        Check(filtered.Rows.Count == 1 && Convert.ToInt32(filtered.Rows[0]["id"]) == 42,
            "Unicode-prefixed WHERE and IN literals preserve filtering");
        var row = filtered.Rows[0];
        Check((string)row["author"] == "O'Brien" && (string)row["label"] == "中文" &&
            (string)row["empty_value"] == "" && (string)row["literal_text"] == "N'CH'" &&
            (string)row["markers"] == "-- /* dbo.source ;" && (string)row["N"] == "EUR",
            "Unicode prefix conversion preserves escapes, empty strings, ordinary literals and identifiers");
    }
    string noViewsPath = Path.Combine(directory, "no-views.db");
    Check(SqlServerToSQLite.CreateSQLiteDatabase(noViewsPath, null, false).ViewErrors.Count == 0, "View migration remains optional");
    using (var db = DbAccessFactory.Create($"Data Source={noViewsPath};Pooling=False", "SQLITE"))
        Check(db.ShowViews().Count == 0 && db.ShowTables().Count == 1, "Disabled view migration creates only tables");
    Console.WriteLine("PASS: SQLite view migration, dependencies, live queries, unsupported SQL, conflicts and optional migration.");

    using (var db = Open())
        Check(db.ShowTables().Count == 0, "Factory must not create configuration tables in a user database");
    Check(SqlQueryBuilder.BuildQuery(new[] { "odd]column" }, "table", "SQLSERVER", 5) == "SELECT TOP 5 [odd]]column] FROM [table]", "SQL Server quoting and limit");
    Check(SqlQueryBuilder.BuildQuery(new[] { "odd`column" }, "table", "SQLITE", 5) == "SELECT `odd``column` FROM `table` LIMIT 5", "SQLite quoting and limit");
    Console.WriteLine("PASS: factory has no configuration-table side effects; SQL dialect limits and escaping.");

    Execute("CREATE TABLE typed (id INTEGER PRIMARY KEY, text TEXT, amount REAL, bytes BLOB, nullable TEXT)");
    var rows = new DataTable();
    rows.Columns.Add("id", typeof(long));
    rows.Columns.Add("text", typeof(string));
    rows.Columns.Add("amount", typeof(double));
    rows.Columns.Add("bytes", typeof(byte[]));
    rows.Columns.Add("nullable", typeof(string));
    rows.Rows.Add(1L, "中文,\"quoted\"\nline", 12.5, new byte[] { 0, 1, 255 }, DBNull.Value);
    rows.Rows.Add(2L, "", -1.25, new byte[] { 2 }, "present");
    using (var db = Open()) using (var reader = rows.CreateDataReader()) db.BulkInsert("typed", reader);
    using (var db = Open())
    {
        var actual = db.GetDataTable("SELECT * FROM typed ORDER BY id");
        Check(actual.Rows.Count == 2 && (string)actual.Rows[0]["text"] == (string)rows.Rows[0]["text"], "Text roundtrip");
        Check(actual.Rows[0]["nullable"] == DBNull.Value, "NULL roundtrip");
        Check(((byte[])actual.Rows[0]["bytes"]).SequenceEqual(new byte[] { 0, 1, 255 }), "Binary roundtrip");
        Check(Convert.ToDouble(actual.Rows[0]["amount"]) == 12.5, "Numeric roundtrip");
    }
    Console.WriteLine("PASS: SQLite Reader import preserves text, numeric, binary and NULL values.");

    var invalid = rows.Clone();
    invalid.Rows.Add(3L, "new", 0.0, new byte[] { 3 }, DBNull.Value);
    invalid.Rows.Add(1L, "duplicate", 0.0, new byte[] { 4 }, DBNull.Value);
    bool failed = false;
    try { using var db = Open(); using var reader = invalid.CreateDataReader(); db.BulkInsert("typed", reader); }
    catch (SQLiteException) { failed = true; }
    Check(failed && Count("typed") == 2, "Failed import must roll back all rows");
    Execute("CREATE TABLE mapped (text TEXT, id INTEGER)");
    using (var projected = rows.DefaultView.ToTable(false, "text", "id"))
    using (var reader = projected.CreateDataReader())
    using (var db = Open()) db.BulkInsert("mapped", reader);
    Check(Count("mapped") == 2, "Projected Reader import");
    Execute("CREATE TABLE \"odd\"\"table\" (\"odd\"\"column\" TEXT)");
    var quoted = new DataTable(); quoted.Columns.Add("odd\"column"); quoted.Rows.Add("value");
    using (var db = Open()) using (var reader = quoted.CreateDataReader()) db.BulkInsert("odd\"table", reader);
    Console.WriteLine("PASS: transaction rollback, projected Reader import and quoted identifiers.");

    Execute("CREATE TABLE large (id INTEGER)");
    var large = new DataTable(); large.Columns.Add("id", typeof(int));
    for (int i = 0; i < 100000; i++) large.Rows.Add(i);
    var watch = Stopwatch.StartNew();
    using (var db = Open()) using (var reader = large.CreateDataReader()) db.BulkInsert("large", reader);
    Console.WriteLine($"INFO: imported 100,000 integer rows in {watch.Elapsed.TotalSeconds:F2}s (not a before/after benchmark).");
    using (var db = Open())
    {
        var preview = QueryPreview.Load(db, "SELECT id FROM large", 1000, out bool truncated);
        Check(preview.Rows.Count == 1000 && truncated, "Bounded preview");
    }
    using (var db = Open())
    {
        var preview = QueryPreview.Load(db, "SELECT id FROM large LIMIT 1000", 1000, out bool truncated);
        Check(preview.Rows.Count == 1000 && !truncated, "Exact-limit preview must not report truncation");
    }
    using (var db = Open())
    {
        var preview = QueryPreview.Load(db, "SELECT id AS same, id AS same FROM large LIMIT 1", 1000, out _);
        Check(preview.Columns.Count == 2 && preview.Columns[0].ColumnName != preview.Columns[1].ColumnName, "Duplicate preview headers");
    }
    Console.WriteLine("PASS: 100,000-row query limited to 1,000; exact limit and duplicate columns.");

    using (var db = Open())
    {
        using (var reader = db.GetDataReader("SELECT id FROM large LIMIT 1")) Check(reader.Read(), "Reader row");
        Check(db.conn.State == ConnectionState.Closed, "Reader disposal closes connection");
        using (var reader = db.GetDataReader("SELECT id FROM large LIMIT 1"))
            Check(db.conn.State == ConnectionState.Open && reader.Read(), "Connection remains open while reading");
        Check(db.conn.State == ConnectionState.Closed, "Reused connection closes after reading");
        try { using var reader = db.GetDataReader("SELECT * FROM missing"); }
        catch (SQLiteException) { }
        Check(db.conn.State == ConnectionState.Closed, "Failed reader creation closes connection");
        db.conn.Open();
        db.ExecuteSql("SELECT 1");
        Check(db.conn.State == ConnectionState.Closed, "Execution accepts an already-open connection and closes it");
        try { db.ExecuteSql("INVALID SQL"); }
        catch (SQLiteException) { }
        Check(db.conn.State == ConnectionState.Closed, "Failed execution closes connection");
        using var recovered = db.GetDataTable("SELECT 1");
        Check(recovered.Rows.Count == 1 && db.conn.State == ConnectionState.Closed, "Query succeeds after failure and closes connection");
    }
    Console.WriteLine("PASS: Reader ownership, connection reuse and failed-query cleanup.");

    Execute("CREATE TABLE csv_target (id INTEGER, text TEXT)");
    string csv = Path.Combine(directory, "input.csv");
    File.WriteAllText(csv, "id,text\r\n1,\"hello, world\"\r\n2,中文\r\n", new UTF8Encoding(true));
    using (var db = Open()) ExcelIO.CsvImport(csv, "csv_target", db);
    Check(Count("csv_target") == 2, "CSV Reader import");

    var csvRows = new DataTable();
    csvRows.Columns.Add("name,\"quoted\"");
    csvRows.Rows.Add("中文,\"value\"\nline");
    csvRows.Rows.Add("second");
    csvRows.Rows.Add("third");
    string csvOutput = Path.Combine(directory, "output.csv");
    using (var reader = csvRows.CreateDataReader())
    {
        CsvExporter.SaveCsv(reader, csvOutput);
        Check(!reader.IsClosed, "CSV caller owns reader");
    }
    using (var stream = File.OpenRead(csvOutput))
    using (var reader = ExcelReaderFactory.CreateCsvReader(stream, new ExcelReaderConfiguration { FallbackEncoding = Encoding.GetEncoding("gb2312") }))
    {
        Check(reader.Read() && reader.GetString(0) == csvRows.Columns[0].ColumnName, "CSV header escaping");
        Check(reader.Read() && reader.GetString(0) == (string)csvRows.Rows[0][0], "CSV value escaping");
    }
    using (var reader = csvRows.CreateDataReader()) CsvExporter.SaveCsv(reader, csvOutput, 2);
    for (int part = 1; part <= 2; part++)
    {
        using var stream = File.OpenRead(Path.Combine(directory, $"output{part}.csv"));
        using var reader = ExcelReaderFactory.CreateCsvReader(stream, new ExcelReaderConfiguration { FallbackEncoding = Encoding.GetEncoding("gb2312") });
        int rowCount = 0;
        while (reader.Read()) rowCount++;
        Check(rowCount == (part == 1 ? 3 : 2), "CSV split includes one header and bounded data rows");
    }
    Console.WriteLine("PASS: CSV header/value escaping, split boundaries and caller-owned reader.");

    var tables = new List<string>();
    for (int i = 0; i < 120; i++)
    {
        string table = "sheet" + i;
        Execute($"CREATE TABLE {table} (id INTEGER, text TEXT)");
        if (i != 0) Execute($"INSERT INTO {table} VALUES ({i}, 'value{i}')");
        tables.Add(table);
    }
    var tracker = new ConnectionTracker();
    using (var db = Open())
    {
        var schema = db.ShowDbSchema();
        Check(schema.Tables.Single(t => t.Name == "sheet0").Columns.Count == 2, "Batched SQLite schema");
        Check(schema.Tables.Single(t => t.Name == "typed").Columns.Count == 5, "Typed table schema");
    }
    using (var tracked = TrackingAccess.Wrap(Open(), tracker))
        ExcelIO.ExportSheetsWithMiniExcel(tables, Path.Combine(directory, "many.xlsx"), tracked, "SQLITE");
    Check(tracker.Peak == 1 && tracker.Active == 0, $"Connections must be bounded: peak {tracker.Peak}, active {tracker.Active}");
    using (var stream = File.OpenRead(Path.Combine(directory, "many.xlsx")))
    using (var reader = ExcelReaderFactory.CreateReader(stream))
    {
        int sheets = 0;
        do
        {
            Check(reader.Name == tables[sheets], "Sheet order");
            Check(reader.Read() && reader.GetValue(0)?.ToString() == "id" && reader.GetValue(1)?.ToString() == "text", "Headers including empty sheets");
            if (sheets == 0) Check(!reader.Read(), "Empty sheet");
            else Check(reader.Read() && Convert.ToInt32(reader.GetValue(0)) == sheets && reader.GetValue(1)?.ToString() == "value" + sheets, "Sheet data");
            sheets++;
        } while (reader.NextResult());
        Check(sheets == 120, "All sheets exported");
    }
    Console.WriteLine("PASS: 120-sheet XLSX roundtrip, empty headers preserved; peak active connections = 1.");

    using (var db = Open()) ExcelIO.ExportSheetsWithEpplus(new[] { "csv_target" }, Path.Combine(directory, "legacy.xlsx"), db, "SQLITE");
    Execute("CREATE TABLE xlsx_target (id INTEGER, text TEXT)");
    foreach (var import in new Action<string, string, IDbAccess>[] { ExcelIO.MiniExcelReaderImport, ExcelIO.ExcelDataReaderImport })
    {
        Execute("DELETE FROM xlsx_target");
        using (var db = Open()) import(Path.Combine(directory, "legacy.xlsx"), "xlsx_target", db);
        Check(Count("xlsx_target") == 2, "Single worksheet imports even when its name differs from the target table");

        Execute("DELETE FROM xlsx_target");
        failed = false;
        try { using var db = Open(); import(Path.Combine(directory, "many.xlsx"), "xlsx_target", db); }
        catch (ArgumentException) { failed = true; }
        Check(failed && Count("xlsx_target") == 0, "Multiple worksheets require a matching name and must not import the first sheet");
        using (var stream = File.Open(Path.Combine(directory, "many.xlsx"), FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            Check(stream.CanRead, "Failed worksheet selection releases the input file");

        Execute("DELETE FROM sheet119");
        using (var db = Open()) import(Path.Combine(directory, "many.xlsx"), "sheet119", db);
        using (var db = Open())
        {
            var actual = db.GetDataTable("SELECT id, text FROM sheet119");
            Check(actual.Rows.Count == 1 && Convert.ToInt32(actual.Rows[0]["id"]) == 119 &&
                (string)actual.Rows[0]["text"] == "value119", "Multiple worksheets select the matching non-first sheet");
        }
    }
    Console.WriteLine("PASS: both importers accept a single differently named sheet, select a matching sheet, and reject missing sheets in multi-sheet workbooks.");
    using (var db = Open())
    using (var reader = db.GetDataReader("SELECT * FROM csv_target"))
    {
        ExcelIO.SaveExcel(Path.Combine(directory, "legacy.xlsx"), reader, "xlsx_target");
        Check(!reader.IsClosed, "Excel caller owns reader");
    }
    using (var db = Open()) ExcelIO.MiniExcelReaderImport(Path.Combine(directory, "legacy.xlsx"), "xlsx_target", db);
    Check(Count("xlsx_target") == 2, "XLSX Reader import and legacy export entry point");
    using (var db = Open()) ExcelIO.ExcelDataReaderImport(Path.Combine(directory, "legacy.xlsx"), "xlsx_target", db);
    Check(Count("xlsx_target") == 4, "ExcelDataReader imports the requested worksheet");
    failed = false;
    try
    {
        using var tracked = TrackingAccess.Wrap(Open(), tracker);
        ExcelIO.ExportSheetsWithMiniExcel(new[] { "sheet1", "missing" }, Path.Combine(directory, "failed.xlsx"), tracked, "SQLITE");
    }
    catch { failed = true; }
    Check(failed && tracker.Active == 0, "Export failure must release connections");
    Console.WriteLine("PASS: CSV/XLSX import, both export entry points, and failure cleanup.");
    Console.WriteLine("All regression checks passed.");
}
finally
{
    // Only this run's GUID-named temporary directory is removed.
    string resolved = Path.GetFullPath(directory);
    if (Path.GetDirectoryName(resolved) != Path.TrimEndingDirectorySeparator(Path.GetFullPath(Path.GetTempPath())) ||
        !Path.GetFileName(resolved).StartsWith("DataPie-regression-", StringComparison.Ordinal))
        throw new InvalidOperationException("Unexpected temporary cleanup path.");
    Directory.Delete(resolved, true);
}

public class ConnectionTracker { public int Active; public int Peak; }
public class TrackingAccess : DispatchProxy
{
    private IDbAccess inner;
    private ConnectionTracker tracker;
    private bool counted;
    public static IDbAccess Wrap(IDbAccess inner, ConnectionTracker tracker)
    {
        var proxy = Create<IDbAccess, TrackingAccess>();
        var state = (TrackingAccess)(object)proxy;
        state.inner = inner; state.tracker = tracker;
        return proxy;
    }
    protected override object Invoke(MethodInfo method, object[] args)
    {
        if (method.Name == "CreateNewAccess") return Wrap(inner.CreateNewAccess(), tracker);
        if (method.Name == "GetDataReader" && !counted)
        {
            counted = true;
            tracker.Active++;
            tracker.Peak = Math.Max(tracker.Peak, tracker.Active);
            if (tracker.Active > 1) throw new Exception("Exporter opened more than one source connection.");
        }
        try { return method.Invoke(inner, args); }
        catch (TargetInvocationException ex)
        {
            ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
            throw;
        }
        finally
        {
            if (method.Name == "Dispose" && counted) { tracker.Active--; counted = false; }
        }
    }
}
