using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DBUtil;
using System.Data.SQLite;
using System.Text.RegularExpressions;
using System.Data.SqlClient;
using System.Data;

namespace DataPieCore
{
    public sealed class SQLiteMigrationResult
    {
        public List<string> ViewErrors { get; } = new List<string>();
        public List<string> ProcedureErrors { get; } = new List<string>();
    }

   public class SqlServerToSQLite
    {

        public static string sqlConnString;

        public static DbSchema dbs;

        public static volatile bool _cancelled = false;

        public static volatile bool Done = false;

        public static volatile string currentProcessTable;

        public static volatile int TotalCopyed;

        /// <summary>
        /// Copies table rows from the SQL Server database to the SQLite database.
        /// </summary>
        /// <param name="sqlConnString">The SQL Server connection string</param>
        /// <param name="sqlitePath">The path to the SQLite database file.</param>
        /// <param name="schema">The schema of the SQL Server database.</param>
        /// <param name="password">The password to use for encrypting the file</param> 
        /// <param name="commandTimeoutSeconds">SQL Server query timeout in seconds; zero disables the timeout.</param>
        public static void CopySqlServerRowsToSQLiteDB(string sqlConnString, string sqlitePath, string password, int commandTimeoutSeconds = 600)
        {
            if (commandTimeoutSeconds < 0) throw new ArgumentOutOfRangeException(nameof(commandTimeoutSeconds));
            Done = false;
            TotalCopyed = 0;
            currentProcessTable = null;
            try
            {
                CheckCancelled();
                using var source = new SqlConnection(sqlConnString);
                source.Open();
                using var destination = new SQLiteConnection(CreateSQLiteConnectionString(sqlitePath, password));
                destination.Open();
                foreach (var table in dbs.DbTables)
                {
                    currentProcessTable = table.TableSchemaName + "." + table.Name;
                    using var query = new SqlCommand(BuildSqlServerTableQuery(table), source)
                    {
                        CommandTimeout = commandTimeoutSeconds
                    };
                    using var reader = query.ExecuteReader(CommandBehavior.SequentialAccess);
                    using var insert = BuildSQLiteInsert(table);
                    insert.Connection = destination;
                    var parameters = insert.Parameters.Cast<SQLiteParameter>().ToArray();
                    var types = table.Columns.Select(GetDbTypeOfColumn).ToArray();
                    SQLiteTransaction transaction = destination.BeginTransaction();
                    try
                    {
                        insert.Transaction = transaction;
                        insert.Prepare();
                        int batchRows = 0;
                        while (reader.Read())
                        {
                            CheckCancelled();
                            for (int j = 0; j < parameters.Length; j++)
                                parameters[j].Value = CastValueForColumn(reader[j], types[j]) ?? DBNull.Value;
                            insert.ExecuteNonQuery();
                            batchRows++;
                            if (batchRows == 1000)
                            {
                                transaction.Commit();
                                TotalCopyed += batchRows;
                                batchRows = 0;
                                transaction.Dispose();
                                transaction = destination.BeginTransaction();
                                insert.Transaction = transaction;
                            }
                        }
                        CheckCancelled();
                        transaction.Commit();
                        TotalCopyed += batchRows;
                    }
                    finally { transaction.Dispose(); }
                }
            }
            catch (SqlException ex) when (ex.Number == -2)
            {
                string operation = currentProcessTable == null
                    ? "opening the SQL Server connection"
                    : $"reading table '{currentProcessTable}' (query timeout: {commandTimeoutSeconds}s)";
                throw new TimeoutException($"Migration timed out while {operation}. " +
                    $"Rows committed across all tables: {TotalCopyed}. The SQLite file is incomplete; " +
                    "previous batches remain committed. Restart the full migration after resolving the timeout.", ex);
            }
            finally { Done = true; }
        }
        /// <summary>
        /// Used in order to adjust the value received from SQL Servr for the SQLite database.
        /// </summary>
        /// <param name="val">The value object</param>
        /// <param name="columnSchema">The corresponding column schema</param>
        /// <returns>SQLite adjusted value.</returns>
        private static object CastValueForColumn(object val, DbType dt)
        {
            if (val is DBNull)
                return null;



            switch (dt)
            {
                case DbType.Int32:
                    if (val is short)
                        return (int)(short)val;
                    if (val is byte)
                        return (int)(byte)val;
                    if (val is long)
                        return (int)(long)val;
                    if (val is decimal)
                        return (int)(decimal)val;
                    break;

                case DbType.Int16:
                    if (val is int)
                        return (short)(int)val;
                    if (val is byte)
                        return (short)(byte)val;
                    if (val is long)
                        return (short)(long)val;
                    if (val is decimal)
                        return (short)(decimal)val;
                    break;

                case DbType.Int64:
                    if (val is int)
                        return (long)(int)val;
                    if (val is short)
                        return (long)(short)val;
                    if (val is byte)
                        return (long)(byte)val;
                    if (val is decimal)
                        return (long)(decimal)val;
                    break;

                case DbType.Single:
                    if (val is double)
                        return (float)(double)val;
                    if (val is decimal)
                        return (float)(decimal)val;
                    break;

                case DbType.Double:
                    if (val is float)
                        return (double)(float)val;
                    if (val is double)
                        return (double)val;
                    if (val is decimal)
                        return (double)(decimal)val;
                    break;

                case DbType.String:
                    if (val is Guid)
                        return ((Guid)val).ToString();
                    break;

                case DbType.Guid:
                    if (val is string)
                        return ParseStringAsGuid((string)val);
                    if (val is byte[])
                        return ParseBlobAsGuid((byte[])val);
                    break;

                case DbType.Byte:
                case DbType.Binary:
                case DbType.Boolean:
                case DbType.DateTime:
                    break;

                default:
                    throw new ArgumentException("Illegal database type [" + Enum.GetName(typeof(DbType), dt) + "]");
            } // switch

            return val;
        }

        private static Guid ParseStringAsGuid(string str)
        {
            try
            {
                return new Guid(str);
            }
            catch
            {
                return Guid.Empty;
            } // catch
        }

        private static Guid ParseBlobAsGuid(byte[] blob)
        {
            byte[] data = blob;
            if (blob.Length > 16)
            {
                data = new byte[16];
                for (int i = 0; i < 16; i++)
                    data[i] = blob[i];
            }
            else if (blob.Length < 16)
            {
                data = new byte[16];
                for (int i = 0; i < blob.Length; i++)
                    data[i] = blob[i];
            }

            return new Guid(data);
        }


        /// <summary>
        /// Builds a SELECT query for a specific table. Needed in the process of copying rows
        /// from the SQL Server database to the SQLite database.
        /// </summary>
        /// <param name="ts">The table schema of the table for which we need the query.</param>
        /// <returns>The SELECT query for the table.</returns>
        private static string BuildSqlServerTableQuery(TableStruct ts)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT ");
            for (int i = 0; i < ts.Columns.Count; i++)
            {
                sb.Append("[" + ts.Columns[i].Name + "]");
                if (i < ts.Columns.Count - 1)
                    sb.Append(", ");
            } // for

            sb.Append(" FROM " + ts.TableSchemaName + "." + "[" + ts.Name + "]");

            return sb.ToString();
        }

        /// <summary>
        /// Creates a command object needed to insert values into a specific SQLite table.
        /// </summary>
        /// <param name="ts">The table schema object for the table.</param>
        /// <returns>A command object with the required functionality.</returns>
        private static SQLiteCommand BuildSQLiteInsert(TableStruct ts)
        {
            SQLiteCommand res = new SQLiteCommand();

            StringBuilder sb = new StringBuilder();
            sb.Append("INSERT INTO [" + ts.Name + "] (");
            for (int i = 0; i < ts.Columns.Count; i++)
            {
                sb.Append("[" + ts.Columns[i].Name + "]");
                if (i < ts.Columns.Count - 1)
                    sb.Append(", ");
            } // for
            sb.Append(") VALUES (");

            List<string> pnames = new List<string>();
            for (int i = 0; i < ts.Columns.Count; i++)
            {
                string pname = "@p" + i;
                sb.Append(pname);
                if (i < ts.Columns.Count - 1)
                    sb.Append(", ");

                DbType dbType = GetDbTypeOfColumn(ts.Columns[i]);
                SQLiteParameter prm = new SQLiteParameter(pname, dbType, ts.Columns[i].Name);
                res.Parameters.Add(prm);

                // Remember the parameter name in order to avoid duplicates
                pnames.Add(pname);
            } // for
            sb.Append(")");
            res.CommandText = sb.ToString();
            res.CommandType = CommandType.Text;
            return res;
        }

        /// <summary>
        /// Matches SQL Server types to general DB types
        /// </summary>
        /// <param name="cs">The column schema to use for the match</param>
        /// <returns>The matched DB type</returns>
        private static DbType GetDbTypeOfColumn(Column cs)
        {
            if (cs.Type == "tinyint")
                return DbType.Byte;
            if (cs.Type == "int")
                return DbType.Int32;
            if (cs.Type == "smallint")
                return DbType.Int16;
            if (cs.Type == "bigint")
                return DbType.Int64;
            if (cs.Type == "bit")
                return DbType.Boolean;
            if (cs.Type == "nvarchar" || cs.Type == "varchar" ||
                cs.Type == "text" || cs.Type == "ntext")
                return DbType.String;
            if (cs.Type == "float")
                return DbType.Double;
            if (cs.Type == "real")
                return DbType.Single;
            if (cs.Type == "blob")
                return DbType.Binary;
            if (cs.Type == "varbinary")
                return DbType.Binary;
            if (cs.Type == "numeric")
                return DbType.Double;
            if (cs.Type == "timestamp" || cs.Type == "datetime" || cs.Type == "datetime2" || cs.Type == "date" || cs.Type == "time")
                return DbType.DateTime;
            if (cs.Type == "nchar" || cs.Type == "char")
                return DbType.String;
            if (cs.Type == "uniqueidentifier" || cs.Type == "guid")
                return DbType.Guid;
            if (cs.Type == "xml")
                return DbType.String;
            if (cs.Type == "sql_variant")
                return DbType.Object;
            if (cs.Type == "integer")
                return DbType.Int64;
            if (cs.Type == "datetimeoffset")
                return DbType.String;
            if (cs.Type == "decimal")
                return DbType.Double;



            throw new ApplicationException("Illegal DB type found (" + cs.Type + ")");
        }

        /// <summary>
        /// Used in order to avoid breaking naming rules (e.g., when a table has
        /// a name in SQL Server that cannot be used as a basis for a matching index
        /// name in SQLite).
        /// </summary>
        /// <param name="str">The name to change if necessary</param>
        /// <param name="names">Used to avoid duplicate names</param>
        /// <returns>A normalized name</returns>
        private static string GetNormalizedName(string str, List<string> names)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < str.Length; i++)
            {
                if (Char.IsLetterOrDigit(str[i]) || str[i] == '_')
                    sb.Append(str[i]);
                else
                    sb.Append("_");
            } // for

            // Avoid returning duplicate name
            if (names.Contains(sb.ToString()))
                return GetNormalizedName(sb.ToString() + "_", names);
            else
                return sb.ToString();
        }


        /// <summary>
        /// Creates the SQLite database from the schema read from the SQL Server.
        /// </summary>
        /// <param name="sqlitePath">The path to the generated DB file.</param>
        /// <param name="schema">The schema of the SQL server database.</param>
        /// <param name="password">The password to use for encrypting the DB or null if non is needed.</param>
        /// <param name="handler">A handle for progress notifications.</param>
        public static SQLiteMigrationResult CreateSQLiteDatabase(string sqlitePath, string password, bool createViews)
        {
            var duplicate = dbs.DbTables.GroupBy(table => table.Name, StringComparer.OrdinalIgnoreCase)
                .FirstOrDefault(group => group.Count() > 1);
            if (duplicate != null)
                throw new InvalidOperationException($"Multiple source tables map to SQLite table '{duplicate.Key}'.");

            SQLiteConnection.CreateFile(sqlitePath);
            using var connection = new SQLiteConnection(CreateSQLiteConnectionString(sqlitePath, password));
            connection.Open();
            foreach (var table in dbs.DbTables)
            {
                CheckCancelled();
                AddSQLiteTable(connection, table);
            }
            var result = new SQLiteMigrationResult();
            if (createViews) result.ViewErrors.AddRange(SQLiteViewMigration.Create(connection, dbs));
            result.ProcedureErrors.AddRange(SQLiteProcedureScripts.Export(connection, sqlitePath, dbs));
            return result;
        }


        /// <summary>
        /// Creates SQLite connection string from the specified DB file path.
        /// </summary>
        /// <param name="sqlitePath">The path to the SQLite database file.</param>
        /// <returns>SQLite connection string</returns>
        private static string CreateSQLiteConnectionString(string sqlitePath, string password)
        {
            SQLiteConnectionStringBuilder builder = new SQLiteConnectionStringBuilder();
            builder.DataSource = sqlitePath;
            if (password != null)
                builder.Password = password;
            builder.PageSize = 4096;
            builder.UseUTF16Encoding = true;
            string connstring = builder.ConnectionString;

            return connstring;
        }

        /// <summary>
        /// Creates the CREATE TABLE DDL for SQLite and a specific table.
        /// </summary>
        /// <param name="conn">The SQLite connection</param>
        /// <param name="dt">The table schema object for the table to be generated.</param>
        private static void AddSQLiteTable(SQLiteConnection conn, TableStruct dt)
        {
            // Prepare a CREATE TABLE DDL statement
            string stmt = BuildCreateTableQuery(dt);
            // Execute the query in order to actually create the table.
            SQLiteCommand cmd = new SQLiteCommand(stmt, conn);
            cmd.ExecuteNonQuery();
        }

        /// <summary>
        /// returns the CREATE TABLE DDL for creating the SQLite table from the specified
        /// table schema object.
        /// </summary>
        /// <param name="ts">The table schema object from which to create the SQL statement.</param>
        /// <returns>CREATE TABLE DDL for the specified table.</returns>
        private static string BuildCreateTableQuery(TableStruct ts)
        {
            StringBuilder sb = new StringBuilder();

            sb.Append("CREATE TABLE [" + ts.Name + "] (\n");

            var primaryKeys = ts.Columns.Where(column => column.IsPrimaryKey).ToList();
            bool pkey = false;
            for (int i = 0; i < ts.Columns.Count; i++)
            {
                var col = ts.Columns[i];
                string cline = BuildColumnStatement(col, primaryKeys.Count == 1 && col.IsPrimaryKey, ref pkey);
                sb.Append(cline);
                if (i < ts.Columns.Count - 1)
                    sb.Append(",\n");
            } // foreach

            // add primary keys...
            if (primaryKeys.Count > 0 && !pkey)
            {
                sb.Append(",\n");
                sb.Append("    PRIMARY KEY (");
                sb.Append(string.Join(", ", primaryKeys.Select(column => "[" + column.Name + "]")));
                sb.Append(")\n");
            }
            else
                sb.Append("\n");

            // add foreign keys...
            if (ts.ForeignKeys.Count > 0)
            {
                sb.Append(",\n");
                for (int i = 0; i < ts.ForeignKeys.Count; i++)
                {
                    ForeignKeySchema foreignKey = ts.ForeignKeys[i];
                    string stmt = string.Format("    FOREIGN KEY ([{0}])\n        REFERENCES [{1}]([{2}])",
                                foreignKey.ColumnName, foreignKey.ForeignTableName, foreignKey.ForeignColumnName);

                    sb.Append(stmt);
                    if (i < ts.ForeignKeys.Count - 1)
                        sb.Append(",\n");
                } // for
            }

            sb.Append("\n");
            sb.Append(");\n");

            //// Create any relevant indexes
            //if (ts.Indexes != null)
            //{
            //    for (int i = 0; i < ts.Indexes.Count; i++)
            //    {
            //        string stmt = BuildCreateIndex(ts.TableName, ts.Indexes[i]);
            //        sb.Append(stmt + ";\n");
            //    } // for
            //} // if

            string query = sb.ToString();
            return query;
        }

        /// <summary>
        /// Used when creating the CREATE TABLE DDL. Creates a single row
        /// for the specified column.
        /// </summary>
        /// <param name="col">The column schema</param>
        /// <returns>A single column line to be inserted into the general CREATE TABLE DDL statement</returns>
        private static string BuildColumnStatement(Column col, bool singlePrimaryKey, ref bool pkey)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("\t[" + col.Name + "]\t");

            // Special treatment for IDENTITY columns
            if (col.IsIdentity)
            {
                if (singlePrimaryKey && (col.Type == "tinyint" || col.Type == "int" || col.Type == "smallint" ||
                    col.Type == "bigint" || col.Type == "integer"))
                {
                    sb.Append("integer PRIMARY KEY AUTOINCREMENT");
                    pkey = true;
                }
                else
                    sb.Append("integer");
            }
            else
            {
                if (col.Type == "int")
                    sb.Append("integer");
                else
                {
                    sb.Append(col.Type);
                }
                if (col.MaxLength > 0)
                    sb.Append("(" + col.MaxLength + ")");
            }
            if (!col.IsNullable)
                sb.Append(" NOT NULL");



            string defval = StripParens(col.Default);
            defval = DiscardNational(defval);

            if (defval != string.Empty && defval.ToUpper().Contains("GETDATE"))
            {
                sb.Append(" DEFAULT (CURRENT_TIMESTAMP)");
            }
            else if (defval != string.Empty && IsValidDefaultValue(defval))
                sb.Append(" DEFAULT " + defval);

            return sb.ToString();
        }


        /// <summary>
        /// Strip any parentheses from the string.
        /// </summary>
        /// <param name="value">The string to strip</param>
        /// <returns>The stripped string</returns>
        private static string StripParens(string value)
        {
            Regex rx = new Regex(@"\(([^\)]*)\)");
            Match m = rx.Match(value);
            if (!m.Success)
                return value;
            else
                return StripParens(m.Groups[1].Value);
        }

        /// <summary>
        /// Discards the national prefix if exists (e.g., N'sometext') which is not
        /// supported in SQLite.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns></returns>
        private static string DiscardNational(string value)
        {
            Regex rx = new Regex(@"N\'([^\']*)\'");
            Match m = rx.Match(value);
            if (m.Success)
                return m.Groups[1].Value;
            else
                return value;
        }

        /// <summary>
        /// Check if the DEFAULT clause is valid by SQLite standards
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        private static bool IsValidDefaultValue(string value)
        {
            if (IsSingleQuoted(value))
                return true;

            double testnum;
            if (!double.TryParse(value, out testnum))
                return false;
            return true;
        }

        private static bool IsSingleQuoted(string value)
        {
            value = value.Trim();
            if (value.StartsWith("'") && value.EndsWith("'"))
                return true;
            return false;
        }

        private static void CheckCancelled()
        {
            if (_cancelled)
                throw new ApplicationException("User cancelled the conversion");
        }





    }
}
