using System.Data;
using System.Data.SQLite;
using DBUtil;

void Check(bool value, string message) { if (!value) throw new Exception(message); }
using var input = new DataTable();
foreach (string name in new[] { "number", "amount", "date", "flag", "text" }) input.Columns.Add(name);
input.Rows.Add("", " ", "\t", "\r\n", " ");
input.Rows.Add("0", "1.25", "2026-10-13", "1", "");
input.Rows.Add(DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value);
var types = new Dictionary<string, Type>
{
    ["number"] = typeof(int), ["amount"] = typeof(decimal), ["date"] = typeof(DateTime),
    ["flag"] = typeof(bool), ["text"] = typeof(string)
};
using (var source = input.CreateDataReader())
using (var normalized = (IDataReader)Activator.CreateInstance(typeof(SqlServerDbAccess).Assembly.GetType("DBUtil.ImportDataReader"), source, types))
{
    normalized.Read();
    var values = new object[5];
    normalized.GetValues(values);
    for (int i = 0; i < 4; i++) Check(normalized.IsDBNull(i) && normalized[i] is DBNull && values[i] is DBNull, "Reader typed blanks must be NULL");
    Check((string)normalized["text"] == " ", "Text whitespace must be preserved");
    normalized.Read();
    Check((string)normalized[0] == "0", "Zero must remain unchanged");
    normalized.Dispose();
    Check(!source.IsClosed, "Caller retains reader ownership");
}
string path = Path.Combine(Path.GetTempPath(), "DataPie-import-" + Guid.NewGuid() + ".db");
try
{
    using var db = DbAccessFactory.Create(new SQLiteConnectionStringBuilder { DataSource = path, Pooling = false }.ToString(), "SQLITE");
    db.ExecuteSql("CREATE TABLE target (number INTEGER, amount DECIMAL(10,2), date DATETIME, flag BOOLEAN, text TEXT)");
    using (var source = input.CreateDataReader()) db.BulkInsert("target", source);
    using var result = db.GetDataTable("SELECT number IS NULL, amount IS NULL, date IS NULL, flag IS NULL, text FROM target ORDER BY rowid");
    for (int i = 0; i < 4; i++) Check(Convert.ToInt32(result.Rows[0][i]) == 1, "SQLite typed blanks must be SQL NULL");
    Check((string)result.Rows[0][4] == " " && (string)result.Rows[1][4] == "", "Text blanks must be preserved");
    for (int i = 0; i < 4; i++) Check(Convert.ToInt32(result.Rows[1][i]) == 0, "Nonblank values must remain non-null");
    Check(result.Rows[2][4] is DBNull, "Existing NULL must be preserved");
    db.ExecuteSql("CREATE TABLE required (number INTEGER NOT NULL)");
    using var required = new DataTable();
    required.Columns.Add("number");
    required.Rows.Add(" ");
    bool rejected = false;
    try { using var source = required.CreateDataReader(); db.BulkInsert("required", source); }
    catch (SQLiteException) { rejected = true; }
    Check(rejected, "NOT NULL constraints must still be enforced");
}
finally { File.Delete(path); }
Console.WriteLine("PASS: typed blanks, text preservation, nonblank values, ownership and SQLite constraints.");
