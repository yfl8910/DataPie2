using DataPieCore;
using DBUtil;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace DataPieDesktop
{
    public partial class Main : Form
    {
        private DbSchema databaseSchema;
        private DatabaseConnectionInfo currentConnection;
        private bool operationRunning;
        private bool updatingStatusLayout;
        private bool sqliteExportRunning;
        private int operationVersion;

        private async Task RunOperationAsync(Action work, Action completed = null,
            string startedMessage = null, bool reportSqliteProgress = false)
        {
            if (operationRunning) return;
            operationRunning = true;
            operationVersion++;
            using var progressTimer = new System.Windows.Forms.Timer { Interval = 5000 };
            var inputs = Descendants(mainTabControl)
                .Where(c => c != cancelSqliteExportButton && c != refreshSqliteProgressButton &&
                    (c is ButtonBase || c is TextBoxBase || c is ComboBox || c is ListBox || c is TreeView || c is DataGridView))
                .Select(c => (Control: c, Enabled: c.Enabled)).ToArray();
            foreach (var input in inputs) input.Control.Enabled = false;
            mainToolStrip.Enabled = false;
            try
            {
                if (startedMessage != null) ShowMessage(startedMessage);
                if (reportSqliteProgress)
                {
                    SqlServerToSQLite._cancelled = false;
                    SqlServerToSQLite.Done = false;
                    SqlServerToSQLite.TotalCopyed = 0;
                    SqlServerToSQLite.currentProcessTable = null;
                    sqliteExportRunning = true;
                    progressTimer.Tick += (_, _) => UpdateSqliteProgress();
                    UpdateSqliteProgress();
                    progressTimer.Start();
                }
                try { await Task.Run(work); }
                finally
                {
                    progressTimer.Stop();
                    sqliteExportRunning = false;
                    operationVersion++;
                }
                if (!IsDisposed && !Disposing) completed?.Invoke();
            }
            catch (Exception ex)
            {
                if (!IsDisposed && !Disposing) ShowError(ex);
            }
            finally
            {
                operationRunning = false;
                progressTimer.Stop();
                sqliteExportRunning = false;
                if (!IsDisposed)
                {
                    foreach (var input in inputs) input.Control.Enabled = input.Enabled;
                    mainToolStrip.Enabled = true;
                }
            }
        }

        private void PostOperationMessage(string message)
        {
            int version = operationVersion;
            if (IsDisposed || Disposing) return;
            BeginInvoke(new Action(() =>
            {
                if (operationRunning && version == operationVersion && !IsDisposed && !Disposing)
                    ShowMessage(message);
            }));
        }

        private async Task RunExportAsync(Action<IDbAccess, string> export)
        {
            string connectionString = currentConnection.ConnectionString;
            string databaseType = currentConnection.DatabaseType;
            var watch = new Stopwatch();
            await RunOperationAsync(() =>
            {
                watch.Start();
                using var access = DbAccessFactory.Create(connectionString, databaseType);
                export(access, databaseType);
                watch.Stop();
            }, () => ShowMessage($"Export successful! Time: {watch.Elapsed.TotalSeconds:F1} seconds"),
                "Processing...");
        }

        private static IEnumerable<Control> Descendants(Control parent)
        {
            foreach (Control child in parent.Controls)
            {
                yield return child;
                foreach (var nested in Descendants(child)) yield return nested;
            }
        }

        public Main()
        {
            InitializeComponent();
            ConfigureStatusBar();
        }

        private void ConfigureStatusBar()
        {
            mainStatusStrip.AutoSize = false;
            foreach (var label in new[] { operationMessageStatusLabel, operationDetailsStatusLabel })
            {
                label.AutoSize = false;
                label.Spring = false;
                label.TextChanged += (_, _) => UpdateStatusLayout();
            }
            foreach (TabPage page in mainTabControl.TabPages) page.AutoScroll = true;
            mainStatusStrip.Layout += (_, _) => UpdateStatusLayout();
            mainStatusStrip.FontChanged += (_, _) => UpdateStatusLayout();
            ClientSizeChanged += (_, _) => UpdateStatusLayout();
            DpiChanged += (_, _) => UpdateStatusLayout();
            UpdateStatusLayout();
        }

        private void UpdateStatusLayout()
        {
            if (updatingStatusLayout || IsDisposed || Disposing) return;
            updatingStatusLayout = true;
            try
            {
                bool hasProgress = !string.IsNullOrEmpty(operationDetailsStatusLabel.Text);
                operationDetailsStatusLabel.Visible = hasProgress;
                int width = Math.Max(2, mainStatusStrip.DisplayRectangle.Width - operationMessageStatusLabel.Margin.Horizontal -
                    (hasProgress ? operationDetailsStatusLabel.Margin.Horizontal : 0));
                int progressWidth = hasProgress ? width / 2 : 0;
                int messageWidth = width - progressWidth;
                int height = ((WrappingStatusLabel)operationMessageStatusLabel).MeasureHeight(messageWidth);
                if (hasProgress) height = Math.Max(height, ((WrappingStatusLabel)operationDetailsStatusLabel).MeasureHeight(progressWidth));
                operationMessageStatusLabel.Size = new Size(messageWidth, height);
                operationDetailsStatusLabel.Size = new Size(progressWidth, height);
                operationMessageStatusLabel.ToolTipText = operationMessageStatusLabel.Text;
                operationDetailsStatusLabel.ToolTipText = operationDetailsStatusLabel.Text;
                mainStatusStrip.Height = height + mainStatusStrip.Padding.Vertical +
                    Math.Max(operationMessageStatusLabel.Margin.Vertical, operationDetailsStatusLabel.Margin.Vertical);
                int gap = Math.Max(1, 8 * DeviceDpi / 96);
                mainTabControl.Size = new Size(Math.Max(1, ClientSize.Width - mainTabControl.Left * 2),
                    Math.Max(1, ClientSize.Height - mainStatusStrip.Height - gap - mainTabControl.Top));
            }
            finally { updatingStatusLayout = false; }
        }

        public async Task SwitchConnectionAsync(DatabaseConnectionInfo connection)
        {
            ArgumentNullException.ThrowIfNull(connection);
            if (IsDisposed || Disposing) throw new ObjectDisposedException(nameof(Main));
            if (operationRunning) throw new InvalidOperationException("Please wait for the current operation before switching databases.");
            var watch = new Stopwatch();
            DbSchema schema = null;
            bool switched = false;
            await RunOperationAsync(() =>
            {
                watch.Start();
                using var access = DbAccessFactory.Create(connection.ConnectionString, connection.DatabaseType);
                access.conn.Open();
                schema = access.ShowDbSchema();
                watch.Stop();
            }, () =>
            {
                currentConnection = connection;
                databaseSchema = schema;
                BindDatabaseSchema();
                var previous = queryResultsGridView.DataSource as DataTable;
                queryResultsGridView.DataSource = null;
                previous?.Dispose();
                importFolderPathTextBox.Clear();
                switched = true;
                ShowMessage($"Database loaded successfully! Time: {watch.Elapsed.TotalSeconds:F1} seconds.");
            }, "Loading data...");
            if (!switched) throw new InvalidOperationException(operationMessageStatusLabel.Text);
        }

        private void BindDatabaseSchema()
        {
            var tableNames = databaseSchema.Tables.Select(table => table.Name).ToArray();
            // Separate data sources keep the import and query selections independent.
            importTableComboBox.DataSource = tableNames;
            queryTableComboBox.DataSource = tableNames.ToArray();
            importTableComboBox.SelectedIndex = -1;
            queryTableComboBox.SelectedIndex = -1;
            importTableComboBox.SelectedIndex = tableNames.Length > 0 ? 0 : -1;
            queryTableComboBox.SelectedIndex = tableNames.Length > 0 ? 0 : -1;
            operationMessageStatusLabel.Text = databaseSchema.Name;

            exportObjectsTreeView.BeginUpdate();
            availableProceduresTreeView.BeginUpdate();
            try
            {
                exportObjectsTreeView.Nodes.Clear();
                availableProceduresTreeView.Nodes.Clear();
                exportObjectsTreeView.Nodes.Add(CreateNodeGroup("All Tables：", tableNames));
                exportObjectsTreeView.Nodes.Add(CreateNodeGroup("All Views：", databaseSchema.ViewNames));
                availableProceduresTreeView.Nodes.Add(CreateNodeGroup("Stored Procedure",
                    databaseSchema.Procedures?.Select(proc => proc.Name) ?? Enumerable.Empty<string>()));
                exportObjectsTreeView.ExpandAll();
                availableProceduresTreeView.ExpandAll();
            }
            finally
            {
                exportObjectsTreeView.EndUpdate();
                availableProceduresTreeView.EndUpdate();
            }
            selectedExportObjectsListBox.Items.Clear();
            selectedProceduresListBox.Items.Clear();
            importFilePathTextBox.Clear();
            sqliteOutputFolderTextBox.Clear();
            sqlEditorRichTextBox.Clear();
        }

        private static TreeNode CreateNodeGroup(string title, IEnumerable<string> names)
        {
            var group = new TreeNode(title) { Name = title };
            group.Nodes.AddRange(names.Select(name => new TreeNode(name) { Name = name }).ToArray());
            return group;
        }

        //Template Export
        private async void exportTemplateButton_Click(object sender, EventArgs e)
        {
            var connection = currentConnection;
            string selectedTable = importTableComboBox.Text;
            if (string.IsNullOrWhiteSpace(selectedTable)) return;
            string filename = FileDialogs.ShowSaveDialog(selectedTable, ".xlsx");
            if (filename == null) return;
            await RunOperationAsync(() =>
            {
                using var access = DbAccessFactory.Create(connection.ConnectionString, connection.DatabaseType);
                using var reader = access.GetDataReader(SqlQueryBuilder.BuildSelectAll(selectedTable, connection.DatabaseType) + " where 1=2");
                ExcelIO.SaveMiniExcel(filename, reader, selectedTable);
            });
        }
        //Delete
        private async void clearTableDataButton_Click(object sender, EventArgs e)
        {
            var connection = currentConnection;
            string selectedTable = importTableComboBox.Text;
            if (string.IsNullOrWhiteSpace(selectedTable))
            {
                MessageBox.Show("please choose a table!");
                return;
            }
            if (MessageBox.Show("Confirm the deletion?", "Message", MessageBoxButtons.OKCancel) != DialogResult.OK)
                return;

            var watch = new Stopwatch();
            await RunOperationAsync(() =>
            {
                watch.Start();
                using var access = DbAccessFactory.Create(connection.ConnectionString, connection.DatabaseType);
                access.TruncateTable(selectedTable);
                watch.Stop();
            }, () => ShowMessage($"Delete time: {watch.Elapsed.TotalSeconds:F1} seconds"),
                "Deleting...");
        }

        // Import Excel
        private async void importFileButton_Click(object sender, EventArgs e)
        {
            var connection = currentConnection;
            if (importFilePathTextBox.Text == "" || importTableComboBox.Text == "")
            {
                MessageBox.Show("please choose table and file to import!");
                return;
            }
            string importTable = importTableComboBox.Text;
            string importPath = importFilePathTextBox.Text;
            var watch = new Stopwatch();
            await RunOperationAsync(() =>
            {
                watch.Start();
                using var access = DbAccessFactory.Create(connection.ConnectionString, connection.DatabaseType);
                ImportFile(importTable, importPath, access);
                watch.Stop();
            }, () => ShowMessage($"Import success, Time: {watch.Elapsed.TotalSeconds:F1} seconds"),
                "Processing...");
        }

        private void ImportFile(string DbTableName, string filename, IDbAccess dbaccess)
        {
            string ext = Path.GetExtension(filename).ToLowerInvariant();
            switch (ext)
            {
                case ".xlsx":
                    ExcelIO.MiniExcelReaderImport(filename, DbTableName, dbaccess);
                    break;
                case ".xls":
                    ExcelIO.ExcelDataReaderImport(filename, DbTableName, dbaccess);
                    break;
                case ".csv":
                    ExcelIO.CsvImport(filename, DbTableName, dbaccess);
                    break;
                case ".db":
                    DbImport(filename, DbTableName, dbaccess);
                    break;
                default:
                    throw new NotSupportedException($"Unsupported import file: {filename}");
            }
        }

        private void DbImport(string filePath, string tableName, IDbAccess dbAccess)
        {
            var db = new DBConfig { ProviderName = "SQLITE", DataBase = filePath };
            using var source = DbAccessFactory.Create(db.GetConstring(), "SQLITE");
            using var reader = source.GetDataReader(SqlQueryBuilder.BuildSelectAll(tableName, "SQLITE"));
            dbAccess.BulkInsert(tableName, reader);
        }

        private void browseImportFileButton_Click(object sender, EventArgs e)
        {
            OpenFileDialog openFileDialog = new OpenFileDialog();
            openFileDialog.Filter = "Excel/Csv/Sqlite|*.xlsx;*.xls;*.csv;*.db";

            openFileDialog.RestoreDirectory = true;
            openFileDialog.FilterIndex = 1;
            if (openFileDialog.ShowDialog() == DialogResult.OK)
            {
                importFilePathTextBox.Text = openFileDialog.FileName;
            }
        }

        private void databaseConnectionToolStripButton_Click(object sender, EventArgs e)
        {

            LoginformShow();
        }

        private void LoginformShow()
        {
            var connectionForm = new DatabaseConnectionForm();
            connectionForm.Show();
        }

        private void exitToolStripButton_Click(object sender, EventArgs e)
        {
            Application.Exit();
            System.Environment.Exit(0);
        }

        private void Main_FormClosed(object sender, FormClosedEventArgs e)
        {
            Application.Exit();
            System.Environment.Exit(0);
        }

        //View Data
        private async void previewQueryButton_Click(object sender, EventArgs e)
        {
            if (string.IsNullOrWhiteSpace(sqlEditorRichTextBox.Text) &&
                !GenerateSql(table => SqlWriter.WriteSelect(table, currentConnection.DatabaseType, 1000), "Select SQL generated"))
                return;
            await LoadQueryPreviewAsync(sqlEditorRichTextBox.Text);
        }

        private async Task LoadQueryPreviewAsync(string sql)
        {
            var connection = currentConnection;
            DataTable result = null;
            bool truncated = false;
            var watch = Stopwatch.StartNew();
            await RunOperationAsync(() =>
            {
                using var access = DbAccessFactory.Create(connection.ConnectionString, connection.DatabaseType);
                result = QueryPreview.Load(access, sql, 1000, out truncated);
            }, () =>
            {
                if (result == null) return;
                var previous = queryResultsGridView.DataSource as DataTable;
                queryResultsGridView.DataSource = result;
                previous?.Dispose();
                watch.Stop();
                operationDetailsStatusLabel.Text = $"Preview: {result.Rows.Count} rows, {watch.Elapsed.TotalSeconds:F2}s" +
                    (truncated ? " (limited to 1000 rows; export for all rows)" : "");
            });
        }
        //OUTPUT CSV
        private async void exportTableToCsvButton_Click(object sender, EventArgs e)
        {
            string tableName = queryTableComboBox.Text;
            if (string.IsNullOrWhiteSpace(tableName))
            {
                MessageBox.Show(this, "Please choose a table");
                return;
            }
            string filename = FileDialogs.ShowSaveDialog(tableName, ".csv");
            if (filename != null) await ExportTableToCsvAsync(tableName, filename);
        }

        private Task ExportTableToCsvAsync(string tableName, string filePath)
            => RunExportAsync((access, databaseType) =>
            {
                using var reader = access.GetDataReader(SqlQueryBuilder.BuildSelectAll(tableName, databaseType));
                CsvExporter.SaveCsv(reader, filePath);
            });

        //Export Excel by sql
        private async void exportQueryToExcelButton_Click(object sender, EventArgs e)
        {
            string sql = sqlEditorRichTextBox.Text;
            if (string.IsNullOrWhiteSpace(sql))
            {
                MessageBox.Show(this, "Empty SQL for Export");
                return;
            }
            string sheetName = queryTableComboBox.Text;
            string filename = FileDialogs.ShowSaveDialog(sheetName, ".xlsx");
            if (filename != null) await ExportQueryToExcelAsync(sql, filename, sheetName);
        }

        //Export Excel by tableName
        private async void exportTableToExcelButton_Click(object sender, EventArgs e)
        {
            string tableName = queryTableComboBox.Text;
            if (string.IsNullOrWhiteSpace(tableName))
            {
                MessageBox.Show(this, "Please choose a table");
                return;
            }
            string filename = FileDialogs.ShowSaveDialog(tableName, ".xlsx");
            if (filename != null)
                await ExportQueryToExcelAsync(SqlQueryBuilder.BuildSelectAll(tableName, currentConnection.DatabaseType), filename, tableName);
        }

        private Task ExportQueryToExcelAsync(string sql, string filePath, string sheetName)
        {
            return RunExportAsync((access, databaseType) =>
            {
                using var reader = access.GetDataReader(sql);
                ExcelIO.SaveMiniExcel(filePath, reader, sheetName);
            });
        }

        private void ShowMessage(string message)
        {
            operationDetailsStatusLabel.Text = string.Empty;
            operationMessageStatusLabel.Text = currentConnection?.DisplayName + "-" + message;
            operationMessageStatusLabel.ToolTipText = operationMessageStatusLabel.Text;
            operationMessageStatusLabel.ForeColor = Color.Red;
        }

        private void ShowError(Exception error) => ShowMessage("Error! " + error.Message);

        private static void AddSelectedNode(ListBox list, TreeNode node)
        {
            if (node?.Parent == null || node.Nodes.Count != 0 || list.Items.Contains(node.Text)) return;
            list.Items.Add(node.Text);
        }

        private static void RemoveSelectedItem(ListBox list)
        {
            if (list.SelectedIndex >= 0) list.Items.RemoveAt(list.SelectedIndex);
        }

        private async Task ExportSelectedTablesAsync(string extension, Func<IList<string>, string, Task> export)
        {
            var tableNames = selectedExportObjectsListBox.Items.Cast<string>().ToArray();
            if (tableNames.Length == 0)
            {
                MessageBox.Show(this, "Please choose a table");
                return;
            }
            string filename = FileDialogs.ShowSaveDialog(tableNames[0], extension);
            if (filename != null) await export(tableNames, filename);
        }

        private bool GenerateSql(Func<TableStruct, string> buildSql, string message)
        {
            var table = databaseSchema?.Tables.FirstOrDefault(item => item.Name == queryTableComboBox.Text);
            if (table == null)
            {
                MessageBox.Show(this, "Please choose a table");
                return false;
            }
            sqlEditorRichTextBox.Text = buildSql(table);
            ShowMessage(message);
            return true;
        }

        private void clearExportListButton_Click(object sender, EventArgs e)
        {
            selectedExportObjectsListBox.Items.Clear();
        }

        private void exportObjectsTreeView_NodeMouseDoubleClick(object sender, TreeNodeMouseClickEventArgs e)
        {
            if (e.Button == MouseButtons.Left) AddSelectedNode(selectedExportObjectsListBox, e.Node);
        }

        private void availableProceduresTreeView_NodeMouseDoubleClick(object sender, TreeNodeMouseClickEventArgs e)
        {
            if (e.Button == MouseButtons.Left) AddSelectedNode(selectedProceduresListBox, e.Node);
        }

        private void selectedProceduresListBox_DoubleClick(object sender, EventArgs e)
        {
            RemoveSelectedItem(selectedProceduresListBox);
        }

        private void selectedExportObjectsListBox_DoubleClick(object sender, EventArgs e)
        {
            RemoveSelectedItem(selectedExportObjectsListBox);
        }

        //export muti excel
        private async void exportSheetsWithEpplusButton_Click(object sender, EventArgs e)
        {
            await ExportSelectedTablesAsync(".xlsx", ExportTablesWithEpplusAsync);
        }

        private Task ExportTablesWithEpplusAsync(IList<string> tableNames, string filePath)
            => RunExportAsync((access, databaseType) =>
                ExcelIO.ExportSheetsWithEpplus(tableNames, filePath, access, databaseType));

        private Task ExportTablesWithMiniExcelAsync(IList<string> tableNames, string filePath)
            => RunExportAsync((access, databaseType) =>
                ExcelIO.ExportSheetsWithMiniExcel(tableNames, filePath, access, databaseType));

        //export muti csv

        private async void exportTablesToCsvButton_Click(object sender, EventArgs e)
        {
            await ExportSelectedTablesAsync(".csv", ExportTablesToCsvAsync);
        }

        private Task ExportTablesToCsvAsync(IList<string> tableNames, string filePath)
            => RunExportAsync((access, databaseType) =>
            {
                string directory = Path.GetDirectoryName(filePath) ?? "";
                foreach (string table in tableNames)
                {
                    using var reader = access.GetDataReader(SqlQueryBuilder.BuildSelectAll(table, databaseType));
                    CsvExporter.SaveCsv(reader, Path.Combine(directory, table + ".csv"));
                }
            });

        // run stored procedure 
        private async void executeProceduresButton_Click(object sender, EventArgs e)
        {
            var procedures = selectedProceduresListBox.Items.Cast<string>().ToArray();
            if (procedures.Length == 0)
            {
                MessageBox.Show(this, "Please choose a stored procedure");
                return;
            }
            await ExecuteProceduresAsync(procedures);
        }

        private async Task ExecuteProceduresAsync(IList<string> procs)
        {
            var connection = currentConnection;
            var watch = new Stopwatch();
            var warnings = new List<string>();
            await RunOperationAsync(() =>
            {
                watch.Start();
                using var access = DbAccessFactory.Create(connection.ConnectionString, connection.DatabaseType);
                var scripts = access is SQLiteDbAccess ? access.GetProcs() : new List<Proc>();
                foreach (string name in procs)
                {
                    access.RunProcedure(name);
                    var script = scripts.FirstOrDefault(proc => proc.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
                    if (!string.IsNullOrEmpty(script?.ScriptWarning))
                        warnings.Add(name + ": " + script.ScriptWarning);
                }
                watch.Stop();
            }, () =>
            {
                string message = $"Procedure execution successful! Time: {watch.Elapsed.TotalSeconds:F1} seconds.";
                if (warnings.Count > 0)
                {
                    message = $"Procedure execution completed with skipped SQL. Time: {watch.Elapsed.TotalSeconds:F1} seconds.";
                    MessageBox.Show(this, string.Join(Environment.NewLine, warnings),
                        "Partial procedure execution", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                }
                ShowMessage(message);
            }, "Processing...");
        }

        private void generateSelectSqlButton_Click(object sender, EventArgs e)
        {
            GenerateSql(table => SqlWriter.WriteSelect(table, currentConnection.DatabaseType, 1000), "Select SQL generated");
        }

        private void generateDeleteSqlButton_Click(object sender, EventArgs e)
        {
            GenerateSql(table => SqlWriter.WriteDelete(table, currentConnection.DatabaseType), "Delete SQL generated");
        }

        private void generateUpdateSqlButton_Click(object sender, EventArgs e)
        {
            GenerateSql(table => SqlWriter.WriteUpdate(table, currentConnection.DatabaseType), "Update SQL generated");
        }

        private async void executeSqlButton_Click(object sender, EventArgs e)
        {
            DialogResult result = MessageBox.Show("Confirm  Executation ?", "Message", MessageBoxButtons.OKCancel);

            if (result == DialogResult.OK && sqlEditorRichTextBox.Text.Length > 0)
            {
                await ExecuteSql(sqlEditorRichTextBox.Text);
            }

        }

        private async Task ExecuteSql(string Sql)
        {
            var connection = currentConnection;
            var watch = new Stopwatch();
            int affectedRows = 0;
            await RunOperationAsync(() =>
            {
                watch.Start();
                using var access = DbAccessFactory.Create(connection.ConnectionString, connection.DatabaseType);
                affectedRows = access.ExecuteSql(Sql);
                watch.Stop();
            }, () => ShowMessage($"Execute success, Time: {watch.Elapsed.TotalSeconds:F1} seconds, Affect {affectedRows} Rows"), "Processing...");
        }

        private void browseSqliteOutputFolderButton_Click(object sender, EventArgs e)
        {
            using var dialog = new FolderBrowserDialog
            {
                Description = "Select the SQLite output folder",
                SelectedPath = Directory.Exists(sqliteOutputFolderTextBox.Text) ? sqliteOutputFolderTextBox.Text : string.Empty
            };
            if (dialog.ShowDialog(this) == DialogResult.OK)
                sqliteOutputFolderTextBox.Text = dialog.SelectedPath;
        }

        private async void createSqliteButton_Click(object sender, EventArgs e)
        {
            if (operationRunning) return;
            if (currentConnection?.DatabaseType != "SQLSERVER" || databaseSchema == null)
            {
                MessageBox.Show(this, "Please connect to a SQL Server database first.");
                return;
            }
            if (!Directory.Exists(sqliteOutputFolderTextBox.Text))
            {
                MessageBox.Show(this, "Please select an existing output folder.");
                return;
            }
            if (string.IsNullOrWhiteSpace(databaseSchema.Name) || databaseSchema.Name.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0 ||
                databaseSchema.Name.EndsWith(".") || databaseSchema.Name.EndsWith(" "))
            {
                MessageBox.Show(this, "The database name cannot be used as a folder or file name.");
                return;
            }
            string filename = Path.Combine(sqliteOutputFolderTextBox.Text, databaseSchema.Name, databaseSchema.Name + ".db");
            if (File.Exists(filename) && MessageBox.Show(this, $"Replace the existing database?\n{filename}",
                "Create SQLite", MessageBoxButtons.YesNo, MessageBoxIcon.Warning) != DialogResult.Yes) return;
            await CreateSqlite(filename, null);
        }

        private async Task CreateSqlite(string filename, string password)
        {
            var connection = currentConnection;
            var watch = new Stopwatch();
            SQLiteMigrationResult migration = null;
            await RunOperationAsync(() =>
            {
                watch.Start();
                using var access = DbAccessFactory.Create(connection.ConnectionString, connection.DatabaseType);
                Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(filename)));
                SqlServerToSQLite.dbs = databaseSchema;
                databaseSchema.ViewDefinitions = ((SqlServerDbAccess)access).ReadViewDefinitions();
                databaseSchema.Procedures = ((SqlServerDbAccess)access).ReadProcedureDefinitions();
                migration = SqlServerToSQLite.CreateSQLiteDatabase(filename, password, true);
                SqlServerToSQLite.CopySqlServerRowsToSQLiteDB(access.ConnectionString, filename, password);
                watch.Stop();
            }, () =>
            {
                var viewErrors = migration.ViewErrors;
                string message = $"SQLite creation successful! Time: {watch.Elapsed.TotalSeconds:F1} seconds, Copy Rows: {SqlServerToSQLite.TotalCopyed} ";
                message += $"Views created: {databaseSchema.ViewDefinitions.Count - viewErrors.Count}, failed: {viewErrors.Count}";
                message += $"; procedures fully converted: {databaseSchema.Procedures.Count - migration.ProcedureErrors.Count}, partial/unsupported: {migration.ProcedureErrors.Count}";
                if (viewErrors.Count > 0 || migration.ProcedureErrors.Count > 0)
                {
                    message = "Migration completed with conversion errors. " + message;
                    MessageBox.Show(this, string.Join(Environment.NewLine, viewErrors.Concat(migration.ProcedureErrors)),
                        "Migration errors", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                }
                ShowMessage(message);
            }, "Processing...", reportSqliteProgress: true);
        }

        private Task CheckConnectionAsync()
        {
            UpdateSqliteProgress();
            return Task.CompletedTask;
        }

        private void UpdateSqliteProgress()
        {
            if (!sqliteExportRunning || IsDisposed || Disposing) return;
            string table = SqlServerToSQLite.currentProcessTable ?? "Preparing schema and procedures";
            string state = SqlServerToSQLite._cancelled ? "Stopping" : "Current Table";
            operationDetailsStatusLabel.Text = $"{state}: {table}, Copy Rows: {SqlServerToSQLite.TotalCopyed}";
        }
        private void cancelSqliteExportButton_Click(object sender, EventArgs e)
        {
            if (!sqliteExportRunning) return;
            SqlServerToSQLite._cancelled = true;
            UpdateSqliteProgress();
            operationDetailsStatusLabel.ForeColor = Color.Red;
        }

        private async void refreshSqliteProgressButton_Click(object sender, EventArgs e)
        {
            await CheckConnectionAsync();

        }

        private void aboutToolStripButton_Click(object sender, EventArgs e)
        {
            AboutDataPie about = new AboutDataPie();
            about.Show();
        }

        private async void exportSheetsWithMiniExcelButton_Click(object sender, EventArgs e)
        {
            await ExportSelectedTablesAsync(".xlsx", ExportTablesWithMiniExcelAsync);
        }

        private void browseImportFolderButton_Click(object sender, EventArgs e)
        {
            FolderBrowserDialog folder = new FolderBrowserDialog();
            if (folder.ShowDialog(this) == DialogResult.OK)
            {
                this.importFolderPathTextBox.Text = folder.SelectedPath;
            }
        }

        private async void importFolderButton_Click(object sender, EventArgs e)
        {
            if (importFolderPathTextBox.Text == "" || importTableComboBox.Text == "")
            {
                MessageBox.Show("please choose table and fold to import!");
                return;
            }
            await ImportFolderAsync(importTableComboBox.Text, importFolderPathTextBox.Text);
        }

        private async Task ImportFolderAsync(string DbTableName, string path)
        {
            var connection = currentConnection;
            var watch = new Stopwatch();
            await RunOperationAsync(() =>
            {
                watch.Start();
                using var access = DbAccessFactory.Create(connection.ConnectionString, connection.DatabaseType);
                var files = ImportFiles.GetFiles(path, false, "");
                for (int i = 0; i < files.Count; i++)
                {
                    PostOperationMessage($"Uploading {i + 1} file: {files[i].FullName}");
                    try { ImportFile(DbTableName, files[i].FullName, access); }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException($"Import failed for '{files[i].FullName}': {ex.Message}", ex);
                    }
                }
                watch.Stop();
            }, () => ShowMessage($"Import success, Time: {watch.Elapsed.TotalSeconds:F1} seconds"),
                "Processing...");
        }

        private void addExportItemButton_Click(object sender, EventArgs e)
        {
            AddSelectedNode(selectedExportObjectsListBox, exportObjectsTreeView.SelectedNode);
        }

        private void removeExportItemButton_Click(object sender, EventArgs e)
        {
            RemoveSelectedItem(selectedExportObjectsListBox);
        }

        private void addProcedureButton_Click(object sender, EventArgs e)
        {
            AddSelectedNode(selectedProceduresListBox, availableProceduresTreeView.SelectedNode);
        }

        private void removeProcedureButton_Click(object sender, EventArgs e)
        {
            RemoveSelectedItem(selectedProceduresListBox);
        }
    }
}
