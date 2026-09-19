using System.Data.SQLite;
using DataPieCore;
using DBUtil;

internal static class SchemaMigrationTests
{
    public static void Run(string directory)
    {
        var previous = SqlServerToSQLite.dbs;
        try
        {
            Column Col(string name, bool primary = false, bool identity = false) =>
                new() { Name = name, Type = "int", IsPrimaryKey = primary, IsIdentity = identity, Default = "" };
            SqlServerToSQLite.dbs = new DbSchema
            {
                Tables = new()
                {
                    new() { Name = "parent", Columns = new() { Col("a", true, true), Col("b", true), Col("code") } },
                    new() { Name = "child", Columns = new() { Col("x"), Col("y") }, ForeignKeys = new()
                    {
                        new() { ColumnNames = new() { "x", "y" }, ReferencedTableName = "parent",
                            ReferencedColumnNames = new() { "a", "b" }, OnDelete = "CASCADE", OnUpdate = "CASCADE" }
                    } },
                    new() { Name = "unique_child", Columns = new() { Col("code") }, ForeignKeys = new()
                    {
                        new() { ColumnNames = new() { "code" }, ReferencedTableName = "parent", ReferencedColumnNames = new() { "code" } }
                    } },
                    new() { Name = "identity_pk", Columns = new() { Col("id", true, true) } },
                    new() { Name = "identity_only", Columns = new() { Col("id", false, true) } }
                }
            };
            string path = Path.Combine(directory, "schema.db");
            SqlServerToSQLite.CreateSQLiteDatabase(path, null, false);
            using var connection = new SQLiteConnection($"Data Source={path};Pooling=False;Foreign Keys=True");
            connection.Open();
            void Execute(string sql) { using var command = new SQLiteCommand(sql, connection); command.ExecuteNonQuery(); }
            long Scalar(string sql) { using var command = new SQLiteCommand(sql, connection); return Convert.ToInt64(command.ExecuteScalar()); }
            void Check(bool value, string message) { if (!value) throw new Exception(message); }
            void Reject(string sql)
            {
                try { Execute(sql); }
                catch (SQLiteException) { return; }
                throw new Exception("Expected constraint failure: " + sql);
            }
            Execute("INSERT INTO parent VALUES (1, 2, 10), (1, 3, 11); INSERT INTO child VALUES (1, 2)");
            Reject("INSERT INTO parent VALUES (1, 2, 12)");
            Reject("INSERT INTO child VALUES (1, 9)");
            Execute("UPDATE parent SET b = 4 WHERE b = 2");
            Check(Scalar("SELECT y FROM child") == 4, "Composite foreign key update cascade");
            Execute("DELETE FROM parent WHERE b = 4");
            Check(Scalar("SELECT COUNT(*) FROM child") == 0, "Composite foreign key delete cascade");
            Execute("INSERT INTO unique_child VALUES (11)");
            Reject("INSERT INTO unique_child VALUES (999)");
            Reject("DELETE FROM parent WHERE code = 11");
            Execute("INSERT INTO identity_pk DEFAULT VALUES; INSERT INTO identity_pk DEFAULT VALUES");
            Check(Scalar("SELECT MAX(id) FROM identity_pk") == 2, "Single identity primary key");
            Execute("INSERT INTO identity_only VALUES (1), (1)");
            Check(Scalar("SELECT COUNT(*) FROM pragma_foreign_key_check") == 0, "Valid migrated foreign keys");
            Console.WriteLine("PASS: composite primary/foreign keys, identity, unique referenced keys and cascades.");
        }
        finally { SqlServerToSQLite.dbs = previous; }
    }
}
