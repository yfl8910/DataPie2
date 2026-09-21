using System.Data.SQLite;
using System.Reflection;
using DataPieDesktop;
using DBUtil;

internal static class ConnectionSwitchTests
{
    public static async Task Run(Main form)
    {
        const BindingFlags flags = BindingFlags.Instance | BindingFlags.NonPublic;
        T Field<T>(string name) => (T)typeof(Main).GetField(name, flags).GetValue(form);
        void Check(bool condition, string message) { if (!condition) throw new Exception(message); }
        string directory = Path.Combine(Path.GetTempPath(), "DataPie-switch-" + Guid.NewGuid());
        Directory.CreateDirectory(directory);
        DatabaseConnectionInfo Create(string name)
        {
            string connectionString = new SQLiteConnectionStringBuilder { DataSource = Path.Combine(directory, name + ".db"), Pooling = false }.ToString();
            using var database = new SQLiteConnection(connectionString);
            database.Open();
            using var command = new SQLiteCommand($"CREATE TABLE [{name}] (id INTEGER PRIMARY KEY)", database);
            command.ExecuteNonQuery();
            return new DatabaseConnectionInfo("SQLITE", connectionString, name);
        }
        try
        {
            var first = Create("First");
            var second = Create("Second");
            await form.SwitchConnectionAsync(first);
            var schema = Field<DbSchema>("databaseSchema");
            Check(ReferenceEquals(first, Field<DatabaseConnectionInfo>("currentConnection")), "Successful switch commits the connection");
            Check(Field<ComboBox>("importTableComboBox").Text == "First", "Successful switch binds the new schema");

            var missing = new DatabaseConnectionInfo("SQLITE", new SQLiteConnectionStringBuilder
            {
                DataSource = Path.Combine(directory, "missing.db"), FailIfMissing = true, Pooling = false
            }.ToString(), "Missing");
            bool failed = false;
            try { await form.SwitchConnectionAsync(missing); }
            catch (InvalidOperationException) { failed = true; }
            Check(failed, "Failed connection must be reported to the caller");
            Check(ReferenceEquals(first, Field<DatabaseConnectionInfo>("currentConnection")) && ReferenceEquals(schema, Field<DbSchema>("databaseSchema")), "Failure must retain the original connection and schema");
            Check(Field<ComboBox>("importTableComboBox").Text == "First", "Failure must retain table selection");

            using var release = new ManualResetEventSlim();
            Task active = (Task)typeof(Main).GetMethod("RunOperationAsync", flags).Invoke(form,
                new object[] { (Action)(() => release.Wait()), null, null, false });
            try
            {
                bool rejected = false;
                try { await form.SwitchConnectionAsync(second); }
                catch (InvalidOperationException) { rejected = true; }
                Check(rejected && ReferenceEquals(first, Field<DatabaseConnectionInfo>("currentConnection")), "Busy operations must reject switching without changing state");
            }
            finally { release.Set(); await active; }

            Field<TextBox>("importFolderPathTextBox").Text = "old folder";
            Field<DataGridView>("queryResultsGridView").DataSource = new System.Data.DataTable();
            await form.SwitchConnectionAsync(second);
            Check(Field<ComboBox>("importTableComboBox").Text == "Second", $"Subsequent switch must load the second database: text={Field<ComboBox>("importTableComboBox").Text}, selected={Field<ComboBox>("importTableComboBox").SelectedItem}, schema={string.Join(",", Field<DbSchema>("databaseSchema").Tables.Select(table => table.Name))}");
            Check(Field<TextBox>("importFolderPathTextBox").Text == "" && Field<DataGridView>("queryResultsGridView").DataSource == null, "Successful switch clears stale paths and query results");
            using var other = new Main();
            Check(typeof(Main).GetField("currentConnection", flags).GetValue(other) == null, "Windows must not share connection state");
            Console.WriteLine("PASS: successful, failed and busy connection switches; window state isolation.");
        }
        finally
        {
            foreach (string path in Directory.GetFiles(directory)) File.Delete(path);
            Directory.Delete(directory);
        }
    }
}
