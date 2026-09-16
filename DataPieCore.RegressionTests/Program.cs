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
IDbAccess Open() => IDBFactory.CreateIDB(connectionString, "SQLITE");
void Check(bool condition, string message) { if (!condition) throw new Exception(message); }
void Execute(string sql) { using var db = Open(); db.ExecuteSql(sql); }
long Count(string table) { using var db = Open(); return Convert.ToInt64(db.GetDataTable("SELECT COUNT(*) FROM " + table).Rows[0][0]); }

try
{
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
    try { using var db = Open(); db.BulkInsert("typed", invalid); }
    catch (SQLiteException) { failed = true; }
    Check(failed && Count("typed") == 2, "Failed import must roll back all rows");
    Execute("CREATE TABLE mapped (text TEXT, id INTEGER)");
    using (var db = Open()) db.BulkInsert("mapped", rows, new[] { "text", "id" });
    Check(Count("mapped") == 2, "Mapped DataTable import");
    Execute("CREATE TABLE \"odd\"\"table\" (\"odd\"\"column\" TEXT)");
    var quoted = new DataTable(); quoted.Columns.Add("odd\"column"); quoted.Rows.Add("value");
    using (var db = Open()) db.BulkInsert("odd\"table", quoted);
    Console.WriteLine("PASS: transaction rollback, mapped DataTable import and quoted identifiers.");

    Execute("CREATE TABLE large (id INTEGER)");
    var large = new DataTable(); large.Columns.Add("id", typeof(int));
    for (int i = 0; i < 100000; i++) large.Rows.Add(i);
    var watch = Stopwatch.StartNew();
    using (var db = Open()) db.BulkInsert("large", large);
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
        Check(db.conn.State == ConnectionState.Closed && !db.IsOpen, "Reader disposal closes connection and resets state");
        db.IsKeepConnect = true;
        using (var reader = db.GetDataReader("SELECT id FROM large LIMIT 1")) Check(reader.Read(), "Reusable connection");
        Check(db.conn.State == ConnectionState.Open && db.IsOpen, "Keep-connect contract");
        db.IsKeepConnect = false;
        try { using var reader = db.GetDataReader("SELECT * FROM missing"); }
        catch (SQLiteException) { }
        Check(db.conn.State == ConnectionState.Closed && !db.IsOpen, "Failed reader creation closes connection");
    }
    Console.WriteLine("PASS: Reader ownership, connection reuse and failed-query cleanup.");

    Execute("CREATE TABLE csv_target (id INTEGER, text TEXT)");
    string csv = Path.Combine(directory, "input.csv");
    File.WriteAllText(csv, "id,text\r\n1,\"hello, world\"\r\n2,中文\r\n", new UTF8Encoding(true));
    using (var db = Open()) ExcelIO.CsvImport(csv, "csv_target", db);
    Check(Count("csv_target") == 2, "CSV Reader import");

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
        Check(schema.DbTables.Single(t => t.Name == "sheet0").Columns.Count == 2, "Batched SQLite schema");
        Check(schema.DbTables.Single(t => t.Name == "typed").Columns.Count == 5, "Typed table schema");
    }
    using (var tracked = TrackingAccess.Wrap(Open(), tracker))
        ExcelIO.SaveMutiMiniExcel(tables, Path.Combine(directory, "many.xlsx"), tracked, "SQLITE");
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

    using (var db = Open()) ExcelIO.SaveMutiExcel(new[] { "csv_target" }, Path.Combine(directory, "legacy.xlsx"), db, "SQLITE");
    Execute("CREATE TABLE xlsx_target (id INTEGER, text TEXT)");
    using (var db = Open()) ExcelIO.MiniExcelReaderImport(Path.Combine(directory, "legacy.xlsx"), "xlsx_target", db);
    Check(Count("xlsx_target") == 2, "XLSX Reader import and legacy export entry point");
    failed = false;
    try
    {
        using var tracked = TrackingAccess.Wrap(Open(), tracker);
        ExcelIO.SaveMutiMiniExcel(new[] { "sheet1", "missing" }, Path.Combine(directory, "failed.xlsx"), tracked, "SQLITE");
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
        if (method.Name == "CreateNewIDB") return Wrap(inner.CreateNewIDB(), tracker);
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
