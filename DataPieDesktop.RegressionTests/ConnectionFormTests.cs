using System.Data.SqlClient;
using System.Data.SQLite;
using System.Reflection;
using DataPieDesktop;

internal static class ConnectionFormTests
{
    private static void Check(bool value, string message)
    {
        if (!value) throw new Exception(message);
    }

    public static void Run()
    {
        var config = new DBConfig
        {
            ProviderName = "SQLSERVER", ServerName = "server", DataBase = "catalog",
            ValidataType = "SQL Server", UserName = "user", UserPwd = "a;'\"b"
        };
        var builder = new SqlConnectionStringBuilder(config.GetConstring());
        Check(builder.Password == config.UserPwd, "Password must round-trip without connection-string injection");
        config.ValidataType = "Windows";
        builder = new SqlConnectionStringBuilder(config.GetConstring());
        Check(builder.IntegratedSecurity && builder.UserID == "" && builder.Password == "", "Windows authentication must not retain credentials");
        Check(new SqlConnectionStringBuilder(config.GetSQLmasterConstring()).InitialCatalog == "master", "Database listing uses master");

        string path = Path.Combine(Path.GetTempPath(), Guid.NewGuid() + ".db");
        try
        {
            var type = typeof(DatabaseConnectionForm).Assembly.GetType("DataPieDesktop.ConnectionStore");
            var store = Activator.CreateInstance(type, new object[] { path });
            void Save(Dbinfo record, bool missing = false) => type.GetMethod("Save").Invoke(store, new object[] { record, missing });
            List<Dbinfo> Load() => (List<Dbinfo>)type.GetMethod("Load").Invoke(store, null);
            var record = new Dbinfo { Dbname = "O'Brien", ConnectionStrings = config.GetConstring(), Dbtype = "SQLSERVER" };
            Save(record, true);
            Save(record, true);
            Check(Load().Count == 1, "Automatic save must not duplicate identical connections");
            record.ConnectionStrings = "Data Source=other";
            Save(record, true);
            Check(Load().Count == 2, "Same database name on different servers must be retained");
            var saved = Load()[0];
            saved.ConnectionStrings = "Data Source='a;b'";
            Save(saved);
            Check(Load()[0].ConnectionStrings == saved.ConnectionStrings, "Quoted values must round-trip through updates");
            type.GetMethod("Delete").Invoke(store, new object[] { saved.Id });
            Check(Load().Count == 1, "Delete must only remove the selected record");
            using var connection = new SQLiteConnection(new SQLiteConnectionStringBuilder { DataSource = path }.ToString());
            connection.Open();
            using var command = new SQLiteCommand("INSERT INTO Dbinfo(Dbname, ConnectionStrings, Dbtype) VALUES ('legacy', 'old', 'MYSQL')", connection);
            command.ExecuteNonQuery();
            Check(Load().Count == 1, "Unsupported legacy providers must be hidden");
            command.CommandText = "SELECT COUNT(*) FROM Dbinfo";
            Check(Convert.ToInt32(command.ExecuteScalar()) == 2, "Legacy records must not be deleted");
        }
        finally
        {
            SQLiteConnection.ClearAllPools();
            File.Delete(path);
        }

        using var form = new DatabaseConnectionForm();
        const BindingFlags flags = BindingFlags.Instance | BindingFlags.NonPublic;
        T Field<T>(string name) => (T)typeof(DatabaseConnectionForm).GetField(name, flags).GetValue(form);
        foreach (var field in typeof(DatabaseConnectionForm).GetFields(flags)
            .Where(field => field.DeclaringType == typeof(DatabaseConnectionForm) && typeof(Control).IsAssignableFrom(field.FieldType)))
        {
            var control = (Control)field.GetValue(form);
            Check(control.Name == field.Name, "Control field and designer name must match: " + field.Name);
            Check(!char.IsDigit(field.Name[^1]), "Control must have a meaningful name: " + field.Name);
        }
        string[] buttons =
        {
            "addConnectionButton", "updateConnectionButton", "deleteConnectionButton", "testConnectionButton",
            "connectSavedConnectionButton", "connectSqlServerButton", "connectSqliteButton",
            "loadDatabasesButton", "browseSqliteFileButton", "startLocalSqlServerButton", "stopLocalSqlServerButton"
        };
        foreach (string name in buttons)
        {
            Check(Field<Button>(name).Name == name, "Button field and designer name must match: " + name);
            Check(typeof(DatabaseConnectionForm).GetMethod(name + "_Click", flags) != null, "Missing click handler: " + name);
        }
        Check(Field<Button>("loadDatabasesButton").Text == "Load Databases", "Database listing must not be labeled as a connection test");
        Field<ComboBox>("savedConnectionComboBox").Items.Add(new Dbinfo { Dbname = "sample", ConnectionStrings = "new", Dbtype = "SQLITE" });
        Field<ComboBox>("savedConnectionComboBox").SelectedIndex = 0;
        Check(typeof(DatabaseConnectionForm).Assembly.GetType("DataPieDesktop.AppState") == null, "Connection selection must not depend on global application state");
        Field<ComboBox>("serverNameComboBox").Text = "changed-server";
        var current = (DBConfig)typeof(DatabaseConnectionForm).GetMethod("ReadSqlServerConfig", flags).Invoke(form, new object[] { "changed-db" });
        Check(current.ServerName == "changed-server" && current.DataBase == "changed-db", "Connection must read current UI values");
        Console.WriteLine("PASS: connection storage, parameter escaping, provider filtering and connection selection.");
    }
}
