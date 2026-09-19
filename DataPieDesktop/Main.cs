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
            var inputs = Descendants(tabControl1)
                .Where(c => c != button17 && c != button18 &&
                    (c is ButtonBase || c is TextBoxBase || c is ComboBox || c is ListBox || c is TreeView || c is DataGridView))
                .Select(c => (Control: c, Enabled: c.Enabled)).ToArray();
            foreach (var input in inputs) input.Control.Enabled = false;
            toolStrip1.Enabled = false;
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
                    toolStrip1.Enabled = true;
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
            string connectionString = AppState.ConnectionString;
            string databaseType = AppState.DatabaseType;
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
            statusStrip1.AutoSize = false;
            foreach (var label in new[] { toolStripStatusLabel1, toolStripStatusLabel2 })
            {
                label.AutoSize = false;
                label.Spring = false;
                label.TextChanged += (_, _) => UpdateStatusLayout();
            }
            foreach (TabPage page in tabControl1.TabPages) page.AutoScroll = true;
            statusStrip1.Layout += (_, _) => UpdateStatusLayout();
            statusStrip1.FontChanged += (_, _) => UpdateStatusLayout();
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
                bool hasProgress = !string.IsNullOrEmpty(toolStripStatusLabel2.Text);
                toolStripStatusLabel2.Visible = hasProgress;
                int width = Math.Max(2, statusStrip1.DisplayRectangle.Width - toolStripStatusLabel1.Margin.Horizontal -
                    (hasProgress ? toolStripStatusLabel2.Margin.Horizontal : 0));
                int progressWidth = hasProgress ? width / 2 : 0;
                int messageWidth = width - progressWidth;
                int height = ((WrappingStatusLabel)toolStripStatusLabel1).MeasureHeight(messageWidth);
                if (hasProgress) height = Math.Max(height, ((WrappingStatusLabel)toolStripStatusLabel2).MeasureHeight(progressWidth));
                toolStripStatusLabel1.Size = new Size(messageWidth, height);
                toolStripStatusLabel2.Size = new Size(progressWidth, height);
                toolStripStatusLabel1.ToolTipText = toolStripStatusLabel1.Text;
                toolStripStatusLabel2.ToolTipText = toolStripStatusLabel2.Text;
                statusStrip1.Height = height + statusStrip1.Padding.Vertical +
                    Math.Max(toolStripStatusLabel1.Margin.Vertical, toolStripStatusLabel2.Margin.Vertical);
                int gap = Math.Max(1, 8 * DeviceDpi / 96);
                tabControl1.Size = new Size(Math.Max(1, ClientSize.Width - tabControl1.Left * 2),
                    Math.Max(1, ClientSize.Height - statusStrip1.Height - gap - tabControl1.Top));
            }
            finally { updatingStatusLayout = false; }
        }

        private async void Main_Load(object sender, EventArgs e)
        {
            await LoadDatabaseSchemaAsync();
        }

        public async Task LoadDatabaseSchemaAsync()
        {
            var watch = new Stopwatch();
            DbSchema schema = null;
            await RunOperationAsync(() =>
            {
                watch.Start();
                using var access = DbAccessFactory.Create(AppState.ConnectionString, AppState.DatabaseType);
                schema = access.ShowDbSchema();
                watch.Stop();
            }, () =>
            {
                databaseSchema = schema;
                BindDatabaseSchema();
                ShowMessage($"Database loaded successfully! Time: {watch.Elapsed.TotalSeconds:F1} seconds.");
            }, "Loading data...");
        }

        private void BindDatabaseSchema()
        {
            var tableNames = databaseSchema.Tables.Select(table => table.Name).ToArray();
            // Separate data sources keep the import and query selections independent.
            comboBox1.DataSource = tableNames;
            comboBox2.DataSource = tableNames.ToArray();
            comboBox1.SelectedIndex = tableNames.Length > 0 ? 0 : -1;
            comboBox2.SelectedIndex = tableNames.Length > 0 ? 0 : -1;
            toolStripStatusLabel1.Text = databaseSchema.Name;

            treeView1.BeginUpdate();
            treeView2.BeginUpdate();
            try
            {
                treeView1.Nodes.Clear();
                treeView2.Nodes.Clear();
                treeView1.Nodes.Add(CreateNodeGroup("All Tables：", tableNames));
                treeView1.Nodes.Add(CreateNodeGroup("All Views：", databaseSchema.ViewNames));
                treeView2.Nodes.Add(CreateNodeGroup("Stored Procedure",
                    databaseSchema.Procedures?.Select(proc => proc.Name) ?? Enumerable.Empty<string>()));
                treeView1.ExpandAll();
                treeView2.ExpandAll();
            }
            finally
            {
                treeView1.EndUpdate();
                treeView2.EndUpdate();
            }
            listBox1.Items.Clear();
            listBox2.Items.Clear();
            textBox1.Clear();
            textBox2.Clear();
            richTextBox1.Clear();
        }

        private static TreeNode CreateNodeGroup(string title, IEnumerable<string> names)
        {
            var group = new TreeNode(title) { Name = title };
            group.Nodes.AddRange(names.Select(name => new TreeNode(name) { Name = name }).ToArray());
            return group;
        }

        //Template Export
        private async void button1_Click(object sender, EventArgs e)
        {
            string selectedTable = comboBox1.Text;
            if (string.IsNullOrWhiteSpace(selectedTable)) return;
            string filename = FileDialogs.ShowSaveDialog(selectedTable, ".xlsx");
            if (filename == null) return;
            await RunOperationAsync(() =>
            {
                using var access = DbAccessFactory.Create(AppState.ConnectionString, AppState.DatabaseType);
                using var reader = access.GetDataReader(SqlQueryBuilder.BuildSelectAll(selectedTable, AppState.DatabaseType) + " where 1=2");
                ExcelIO.SaveMiniExcel(filename, reader, selectedTable);
            });
        }
        //Delete
        private async void button2_Click(object sender, EventArgs e)
        {
            string selectedTable = comboBox1.Text;
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
                using var access = DbAccessFactory.Create(AppState.ConnectionString, AppState.DatabaseType);
                access.TruncateTable(selectedTable);
                watch.Stop();
            }, () => ShowMessage($"Delete time: {watch.Elapsed.TotalSeconds:F1} seconds"),
                "Deleting...");
        }

        // Import Excel
        private async void button3_Click(object sender, EventArgs e)
        {
            if (textBox1.Text == "" || comboBox1.Text == "")
            {
                MessageBox.Show("please choose table and file to import!");
                return;
            }
            string importTable = comboBox1.Text;
            string importPath = textBox1.Text;
            var watch = new Stopwatch();
            await RunOperationAsync(() =>
            {
                watch.Start();
                using var access = DbAccessFactory.Create(AppState.ConnectionString, AppState.DatabaseType);
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

        private void BrowseBtn1_Click(object sender, EventArgs e)
        {
            OpenFileDialog openFileDialog = new OpenFileDialog();
            openFileDialog.Filter = "Excel/Csv/Sqlite|*.xlsx;*.xls;*.csv;*.db";

            openFileDialog.RestoreDirectory = true;
            openFileDialog.FilterIndex = 1;
            if (openFileDialog.ShowDialog() == DialogResult.OK)
            {
                textBox1.Text = openFileDialog.FileName;
            }
        }

        private void toolStripButton1_Click(object sender, EventArgs e)
        {

            LoginformShow();
        }

        private void LoginformShow()
        {
            var connectionForm = new DatabaseConnectionForm();
            connectionForm.Show();
        }

        private void toolStripButton2_Click(object sender, EventArgs e)
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
        private async void button5_Click(object sender, EventArgs e)
        {
            if (string.IsNullOrWhiteSpace(richTextBox1.Text) &&
                !GenerateSql(table => SqlWriter.WriteSelect(table, AppState.DatabaseType, 1000), "Select SQL generated"))
                return;
            await LoadQueryPreviewAsync(richTextBox1.Text);
        }

        private async Task LoadQueryPreviewAsync(string sql)
        {
            DataTable result = null;
            bool truncated = false;
            var watch = Stopwatch.StartNew();
            await RunOperationAsync(() =>
            {
                using var access = DbAccessFactory.Create(AppState.ConnectionString, AppState.DatabaseType);
                result = QueryPreview.Load(access, sql, 1000, out truncated);
            }, () =>
            {
                if (result == null) return;
                var previous = dataGridView1.DataSource as DataTable;
                dataGridView1.DataSource = result;
                previous?.Dispose();
                watch.Stop();
                statusStrip1.Items[1].Text = $"Preview: {result.Rows.Count} rows, {watch.Elapsed.TotalSeconds:F2}s" +
                    (truncated ? " (limited to 1000 rows; export for all rows)" : "");
            });
        }
        //OUTPUT CSV
        private async void button7_Click(object sender, EventArgs e)
        {
            string tableName = comboBox2.Text;
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
        private async void exportQueryButton_Click(object sender, EventArgs e)
        {
            string sql = richTextBox1.Text;
            if (string.IsNullOrWhiteSpace(sql))
            {
                MessageBox.Show(this, "Empty SQL for Export");
                return;
            }
            string sheetName = comboBox2.Text;
            string filename = FileDialogs.ShowSaveDialog(sheetName, ".xlsx");
            if (filename != null) await ExportQueryToExcelAsync(sql, filename, sheetName);
        }

        //Export Excel by tableName
        private async void exportTableButton_Click(object sender, EventArgs e)
        {
            string tableName = comboBox2.Text;
            if (string.IsNullOrWhiteSpace(tableName))
            {
                MessageBox.Show(this, "Please choose a table");
                return;
            }
            string filename = FileDialogs.ShowSaveDialog(tableName, ".xlsx");
            if (filename != null)
                await ExportQueryToExcelAsync(SqlQueryBuilder.BuildSelectAll(tableName, AppState.DatabaseType), filename, tableName);
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
            toolStripStatusLabel2.Text = string.Empty;
            toolStripStatusLabel1.Text = AppState.DatabaseName + "-" + message;
            toolStripStatusLabel1.ToolTipText = toolStripStatusLabel1.Text;
            toolStripStatusLabel1.ForeColor = Color.Red;
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
            var tableNames = listBox1.Items.Cast<string>().ToArray();
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
            var table = databaseSchema?.Tables.FirstOrDefault(item => item.Name == comboBox2.Text);
            if (table == null)
            {
                MessageBox.Show(this, "Please choose a table");
                return false;
            }
            richTextBox1.Text = buildSql(table);
            ShowMessage(message);
            return true;
        }

        private void ClearAllTables_Click(object sender, EventArgs e)
        {
            listBox1.Items.Clear();
        }

        private void treeView1_NodeMouseDoubleClick(object sender, TreeNodeMouseClickEventArgs e)
        {
            if (e.Button == MouseButtons.Left) AddSelectedNode(listBox1, e.Node);
        }

        private void treeView2_NodeMouseDoubleClick(object sender, TreeNodeMouseClickEventArgs e)
        {
            if (e.Button == MouseButtons.Left) AddSelectedNode(listBox2, e.Node);
        }

        private void listBox2_DoubleClick(object sender, EventArgs e)
        {
            RemoveSelectedItem(listBox2);
        }

        private void listBox1_DoubleClick(object sender, EventArgs e)
        {
            RemoveSelectedItem(listBox1);
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

        private async void button11_Click(object sender, EventArgs e)
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
        private async void button12_Click(object sender, EventArgs e)
        {
            var procedures = listBox2.Items.Cast<string>().ToArray();
            if (procedures.Length == 0)
            {
                MessageBox.Show(this, "Please choose a stored procedure");
                return;
            }
            await ExecuteProceduresAsync(procedures);
        }

        private async Task ExecuteProceduresAsync(IList<string> procs)
        {
            var watch = new Stopwatch();
            var warnings = new List<string>();
            await RunOperationAsync(() =>
            {
                watch.Start();
                using var access = DbAccessFactory.Create(AppState.ConnectionString, AppState.DatabaseType);
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

        private void button9_Click(object sender, EventArgs e)
        {
            GenerateSql(table => SqlWriter.WriteSelect(table, AppState.DatabaseType, 1000), "Select SQL generated");
        }

        private void button13_Click(object sender, EventArgs e)
        {
            GenerateSql(table => SqlWriter.WriteDelete(table, AppState.DatabaseType), "Delete SQL generated");
        }

        private void button14_Click(object sender, EventArgs e)
        {
            GenerateSql(table => SqlWriter.WriteUpdate(table, AppState.DatabaseType), "Update SQL generated");
        }

        private async void button6_Click(object sender, EventArgs e)
        {
            DialogResult result = MessageBox.Show("Confirm  Executation ?", "Message", MessageBoxButtons.OKCancel);

            if (result == DialogResult.OK && richTextBox1.Text.Length > 0)
            {
                await ExecuteSql(richTextBox1.Text);
            }

        }

        private async Task ExecuteSql(string Sql)
        {
            var watch = new Stopwatch();
            int affectedRows = 0;
            await RunOperationAsync(() =>
            {
                watch.Start();
                using var access = DbAccessFactory.Create(AppState.ConnectionString, AppState.DatabaseType);
                affectedRows = access.ExecuteSql(Sql);
                watch.Stop();
            }, () => ShowMessage($"Execute success, Time: {watch.Elapsed.TotalSeconds:F1} seconds, Affect {affectedRows} Rows"), "Processing...");
        }

        private void button15_Click(object sender, EventArgs e)
        {
            using var dialog = new FolderBrowserDialog
            {
                Description = "Select the SQLite output folder",
                SelectedPath = Directory.Exists(textBox2.Text) ? textBox2.Text : string.Empty
            };
            if (dialog.ShowDialog(this) == DialogResult.OK)
                textBox2.Text = dialog.SelectedPath;
        }

        private async void createSqliteButton_Click(object sender, EventArgs e)
        {
            if (operationRunning) return;
            if (AppState.DatabaseType != "SQLSERVER" || databaseSchema == null)
            {
                MessageBox.Show(this, "Please connect to a SQL Server database first.");
                return;
            }
            if (!Directory.Exists(textBox2.Text))
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
            string filename = Path.Combine(textBox2.Text, databaseSchema.Name, databaseSchema.Name + ".db");
            if (File.Exists(filename) && MessageBox.Show(this, $"Replace the existing database?\n{filename}",
                "Create SQLite", MessageBoxButtons.YesNo, MessageBoxIcon.Warning) != DialogResult.Yes) return;
            await CreateSqlite(filename, null);
        }

        private async Task CreateSqlite(string filename, string password)
        {
            var watch = new Stopwatch();
            SQLiteMigrationResult migration = null;
            await RunOperationAsync(() =>
            {
                watch.Start();
                using var access = DbAccessFactory.Create(AppState.ConnectionString, AppState.DatabaseType);
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
            toolStripStatusLabel2.Text = $"{state}: {table}, Copy Rows: {SqlServerToSQLite.TotalCopyed}";
        }
        private void button17_Click(object sender, EventArgs e)
        {
            if (!sqliteExportRunning) return;
            SqlServerToSQLite._cancelled = true;
            UpdateSqliteProgress();
            toolStripStatusLabel2.ForeColor = Color.Red;
        }

        private async void button18_Click(object sender, EventArgs e)
        {
            await CheckConnectionAsync();

        }

        private void toolStripButton3_Click(object sender, EventArgs e)
        {
            AboutDataPie about = new AboutDataPie();
            about.Show();
        }

        private async void exportSheetsWithMiniExcelButton_Click(object sender, EventArgs e)
        {
            await ExportSelectedTablesAsync(".xlsx", ExportTablesWithMiniExcelAsync);
        }

        private void button21_Click(object sender, EventArgs e)
        {
            FolderBrowserDialog folder = new FolderBrowserDialog();
            if (folder.ShowDialog(this) == DialogResult.OK)
            {
                this.textBox3.Text = folder.SelectedPath;
            }
        }

        private async void button20_Click(object sender, EventArgs e)
        {
            if (textBox3.Text == "" || comboBox1.Text == "")
            {
                MessageBox.Show("please choose table and fold to import!");
                return;
            }
            await ImportFolderAsync(comboBox1.Text, textBox3.Text);
        }

        private async Task ImportFolderAsync(string DbTableName, string path)
        {
            var watch = new Stopwatch();
            await RunOperationAsync(() =>
            {
                watch.Start();
                using var access = DbAccessFactory.Create(AppState.ConnectionString, AppState.DatabaseType);
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

        private void buttonExAdd_Click(object sender, EventArgs e)
        {
            AddSelectedNode(listBox1, treeView1.SelectedNode);
        }

        private void buttonExRemove_Click(object sender, EventArgs e)
        {
            RemoveSelectedItem(listBox1);
        }

        private void btnAddProce_Click(object sender, EventArgs e)
        {
            AddSelectedNode(listBox2, treeView2.SelectedNode);
        }

        private void btnDeleteProc_Click(object sender, EventArgs e)
        {
            RemoveSelectedItem(listBox2);
        }
    }
}
