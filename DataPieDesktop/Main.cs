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
        public static DatabaseConnectionForm loginform = null;



        static DbSchema dbs;

        IList<string> tableList = new List<string>();

        IList<string> viewList = new List<string>();

        IList<string> SpList = new List<string>();

        string tableName = "";


        private Point pi;
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
                if (startedMessage != null) ShowMessage(startedMessage, EventArgs.Empty);
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
                if (!IsDisposed && !Disposing) ShowErr(ex, EventArgs.Empty);
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
                    ShowMessage(message, EventArgs.Empty);
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
            }, () => ShowMessage($"Export successful! Time: {watch.Elapsed.TotalSeconds:F1} seconds", EventArgs.Empty),
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

        private void tabPage1_Click(object sender, EventArgs e)
        {

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
                dbs = schema;
                tableList = dbs.Tables.Select(table => table.Name).ToList();
                viewList = dbs.ViewNames;
                SpList = dbs.Procedures?.Select(proc => proc.Name).ToList() ?? new List<string>();
                SetcomboBox(tableList);
                ShowMessage($"Database loaded successfully! Time: {watch.Elapsed.TotalSeconds:F1} seconds.", EventArgs.Empty);
            }, "Loading data...");
        }


        private void SetcomboBox(object collection)
        {
            this.comboBox1.DataSource = tableList;
            this.comboBox1.SelectedIndex = tableList.Count > 0 ? 0 : -1;

            this.comboBox2.DataSource = tableList;
            this.comboBox2.SelectedIndex = tableList.Count > 0 ? 0 : -1;

            statusStrip1.Items[0].Text = dbs.Name;


            treeView1.BeginUpdate();
            treeView2.BeginUpdate();
            try
            {
            treeView1.Nodes.Clear();

            treeView2.Nodes.Clear();

            TreeNode Node = new TreeNode();

            Node.Name = "All Tables：";
            Node.Text = "All Tables：";
            treeView1.Nodes.Add(Node);

            Node = new TreeNode();
            Node.Name = "All Views：";
            Node.Text = "All Views：";
            treeView1.Nodes.Add(Node);

            foreach (string s in tableList)
            {
                TreeNode tn = new TreeNode();
                tn.Name = s;
                tn.Text = s;
                treeView1.Nodes["All Tables："].Nodes.Add(tn);
            }

            foreach (string s in viewList)
            {
                TreeNode tn = new TreeNode();
                tn.Name = s;
                tn.Text = s;
                treeView1.Nodes["All Views："].Nodes.Add(tn);
            }

            Node = new TreeNode();
            Node.Name = "Stored Procedure";
            Node.Text = "Stored Procedure";
            treeView2.Nodes.Add(Node);


            if (SpList.Count > 0)
            {

                foreach (string s in SpList)
                {
                    TreeNode tn = new TreeNode();
                    tn.Name = s;
                    tn.Text = s;
                    treeView2.Nodes["Stored Procedure"].Nodes.Add(tn);
                }
            }

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

            textBox1.Text = "";

            textBox2.Text = "";

            richTextBox1.Text = "";


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
            }, () => ShowMessage($"Delete time: {watch.Elapsed.TotalSeconds:F1} seconds", EventArgs.Empty),
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
            string importTable = tableName;
            string importPath = textBox1.Text;
            var watch = new Stopwatch();
            await RunOperationAsync(() =>
            {
                watch.Start();
                using var access = DbAccessFactory.Create(AppState.ConnectionString, AppState.DatabaseType);
                ImportFile(importTable, importPath, access);
                watch.Stop();
            }, () => ShowMessage($"Import success, Time: {watch.Elapsed.TotalSeconds:F1} seconds", EventArgs.Empty),
                "Processing...");
        }

        public void ImportFile(string DbTableName, string filename, IDbAccess dbaccess)
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

        public void DbImport(string filePath, string tableName, IDbAccess dbAccess)
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


        private void comboBox1_SelectedIndexChanged(object sender, EventArgs e)
        {
            tableName = comboBox1.Text.ToString();
        }

        private void comboBox2_SelectedIndexChanged(object sender, EventArgs e)
        {
            tableName = comboBox2.Text.ToString();

        }

        private void toolStripButton1_Click(object sender, EventArgs e)
        {

            LoginformShow();
        }



        private void LoginformShow()
        {

            loginform = new DatabaseConnectionForm();
            loginform.Show();

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

            if (richTextBox1.Text.Length > 0)
            {
                await LoadQueryPreviewAsync(richTextBox1.Text);
            }
            else
            {
                string sql = SqlWriter.WriteSelect(dbs.Tables.FirstOrDefault(p => p.Name == tableName), AppState.DatabaseType, 1000);

                richTextBox1.Text = sql;

                await LoadQueryPreviewAsync(sql);
            }

        }

        public async Task LoadQueryPreviewAsync(string sql)
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
            string filename = FileDialogs.ShowSaveDialog(tableName, ".csv");
            if (filename != null) await ExportTableToCsvAsync(tableName, filename);
        }

        public Task ExportTableToCsvAsync(string tableName, string filePath)
            => RunExportAsync((access, databaseType) =>
            {
                using var reader = access.GetDataReader(SqlQueryBuilder.BuildSelectAll(tableName, databaseType));
                CsvExporter.SaveCsv(reader, filePath);
            });



        //Export Excel by sql
        private async void exportQueryButton_Click(object sender, EventArgs e)
        {
            if (richTextBox1.Text.Length == 0)
            {
                MessageBox.Show("Empty SQL for Export");
            }
            else
            {
                string filename = FileDialogs.ShowSaveDialog(tableName, ".xlsx");

                if (filename != null)
                {
                    await ExportQueryToExcelAsync(richTextBox1.Text.ToString(), filename);

                }

            }
        }


        //Export Excel by tableName
        private async void exportTableButton_Click(object sender, EventArgs e)
        {
            if (tableName == "")
            {
                MessageBox.Show("Please choose a table");
            }
            else
            {
                string filename = FileDialogs.ShowSaveDialog(tableName, ".xlsx");

                if (filename != null)
                {
                    string sql = SqlQueryBuilder.BuildSelectAll(tableName, AppState.DatabaseType);

                    await ExportQueryToExcelAsync(sql, filename);

                }

            }
        }

        public Task ExportQueryToExcelAsync(string sql, string filePath)
        {
            string sheetName = tableName;
            return RunExportAsync((access, databaseType) =>
            {
                using var reader = access.GetDataReader(sql);
                ExcelIO.SaveMiniExcel(filePath, reader, sheetName);
            });
        }


        private void ShowMessage(object o, System.EventArgs e)
        {
            toolStripStatusLabel2.Text = string.Empty;
            statusStrip1.Items[0].Text = AppState.DatabaseName + "-" + o.ToString();
            toolStripStatusLabel1.ToolTipText = toolStripStatusLabel1.Text;
            statusStrip1.Items[0].ForeColor = Color.Red;
        }

        private void ShowErr(object o, System.EventArgs e)
        {
            Exception ee = o as Exception;

            ShowMessage("Error! " + ee.Message, e);
        }




        private void ClearAllTables_Click(object sender, EventArgs e)
        {
            listBox1.Items.Clear();
        }

        private void treeView1_DoubleClick(object sender, System.EventArgs e)
        {
            TreeNode node = this.treeView1.GetNodeAt(pi);
            if (pi.X < node.Bounds.Left || pi.X > node.Bounds.Right)
            {
                //不触发事件   
                return;
            }
            else
            {
                int i = treeView1.SelectedNode.GetNodeCount(false);
                if (!listBox1.Items.Contains(treeView1.SelectedNode.Text.ToString()) && i == 0)

                    listBox1.Items.Add(treeView1.SelectedNode.Text.ToString());
            }


        }

        private void treeView1_MouseDown(object sender, System.Windows.Forms.MouseEventArgs e)
        {
            pi = new Point(e.X, e.Y);
        }

        private void treeView2_DoubleClick(object sender, System.EventArgs e)
        {
            TreeNode node = this.treeView2.GetNodeAt(pi);
            if (pi.X < node.Bounds.Left || pi.X > node.Bounds.Right)
            {
                return;
            }
            else
            {
                int i = treeView2.SelectedNode.GetNodeCount(false);
                if (!listBox2.Items.Contains(treeView2.SelectedNode.Text.ToString()) && i == 0)
                    listBox2.Items.Add(treeView2.SelectedNode.Text.ToString());
            }
        }

        private void treeView2_MouseDown(object sender, System.Windows.Forms.MouseEventArgs e)
        {
            pi = new Point(e.X, e.Y);
        }



        private void listBox2_DoubleClick(object sender, EventArgs e)
        {

            listBox2.Items.RemoveAt(listBox2.SelectedIndex);
        }

        private void listBox1_DoubleClick(object sender, EventArgs e)
        {

            listBox1.Items.RemoveAt(listBox1.SelectedIndex);
        }



        //export muti excel
        private async void exportSheetsWithEpplusButton_Click(object sender, EventArgs e)
        {
            if (listBox1.Items.Count < 1)
            {
                MessageBox.Show("please choose a table !");
                return;
            }

            IList<string> SheetNames = new List<string>();

            foreach (var item in listBox1.Items)
            {
                SheetNames.Add(item.ToString());
            }

            string filename = FileDialogs.ShowSaveDialog(SheetNames[0], ".xlsx");

            if (filename != null)
            {
                await ExportTablesWithEpplusAsync(SheetNames, filename);

            }
        }

        public Task ExportTablesWithEpplusAsync(IList<string> tableNames, string filePath)
            => RunExportAsync((access, databaseType) =>
                ExcelIO.ExportSheetsWithEpplus(tableNames, filePath, access, databaseType));


        public Task ExportTablesWithMiniExcelAsync(IList<string> tableNames, string filePath)
            => RunExportAsync((access, databaseType) =>
                ExcelIO.ExportSheetsWithMiniExcel(tableNames, filePath, access, databaseType));


        //export muti csv

        private async void button11_Click(object sender, EventArgs e)
        {
            if (listBox1.Items.Count < 1)
            {
                MessageBox.Show("please choose a table !");
                return;
            }

            IList<string> SheetNames = new List<string>();

            foreach (var item in listBox1.Items)
            {
                SheetNames.Add(item.ToString());
            }

            string filename = FileDialogs.ShowSaveDialog(SheetNames[0], ".csv");

            if (filename != null)
            {
                await ExportTablesToCsvAsync(SheetNames, filename);

            }

        }

        public Task ExportTablesToCsvAsync(IList<string> tableNames, string filePath)
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
            if (listBox2.Items.Count < 1)
            {
                MessageBox.Show("please choose a stored procedure !");
            }
            else
            {
                IList<string> list = new List<string>();
                foreach (var item in listBox2.Items)
                {
                    list.Add(item.ToString());
                }

                await ExecuteProceduresAsync(list);
            }
        }

        public async Task ExecuteProceduresAsync(IList<string> procs)
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
                ShowMessage(message, EventArgs.Empty);
            }, "Processing...");
        }

        private async void button9_Click(object sender, EventArgs e)
        {
            string sql = SqlWriter.WriteSelect(dbs.Tables.FirstOrDefault(p => p.Name == tableName), AppState.DatabaseType, 1000);
            richTextBox1.Text = sql;
            this.BeginInvoke(new System.EventHandler(ShowMessage), "Select Sql Generated");

        }

        private async void button13_Click(object sender, EventArgs e)
        {
            string sql = SqlWriter.WriteDelete(dbs.Tables.FirstOrDefault(p => p.Name == tableName), AppState.DatabaseType);
            richTextBox1.Text = sql;
            this.BeginInvoke(new System.EventHandler(ShowMessage), "Delete Sql Generated");

        }

        private async void button14_Click(object sender, EventArgs e)
        {
            string sql = SqlWriter.WriteUpdate(dbs.Tables.FirstOrDefault(p => p.Name == tableName), AppState.DatabaseType);
            richTextBox1.Text = sql;
            this.BeginInvoke(new System.EventHandler(ShowMessage), "Update Sql Generated");

        }

        private async void button6_Click(object sender, EventArgs e)
        {
            DialogResult result = MessageBox.Show("Confirm  Executation ?", "Message", MessageBoxButtons.OKCancel);

            if (result == DialogResult.OK && richTextBox1.Text.Length > 0)
            {
                await ExecuteSql(richTextBox1.Text);
            }

        }

        public async Task ExecuteSql(string Sql)
        {
            var watch = new Stopwatch();
            int affectedRows = 0;
            await RunOperationAsync(() =>
            {
                watch.Start();
                using var access = DbAccessFactory.Create(AppState.ConnectionString, AppState.DatabaseType);
                affectedRows = access.ExecuteSql(Sql);
                watch.Stop();
            }, () => ShowMessage($"Execute success, Time: {watch.Elapsed.TotalSeconds:F1} seconds, Affect {affectedRows} Rows",
                EventArgs.Empty), "Processing...");
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
            if (AppState.DatabaseType != "SQLSERVER" || dbs == null)
            {
                MessageBox.Show(this, "Please connect to a SQL Server database first.");
                return;
            }
            if (!Directory.Exists(textBox2.Text))
            {
                MessageBox.Show(this, "Please select an existing output folder.");
                return;
            }
            if (string.IsNullOrWhiteSpace(dbs.Name) || dbs.Name.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0 ||
                dbs.Name.EndsWith(".") || dbs.Name.EndsWith(" "))
            {
                MessageBox.Show(this, "The database name cannot be used as a folder or file name.");
                return;
            }
            string filename = Path.Combine(textBox2.Text, dbs.Name, dbs.Name + ".db");
            if (File.Exists(filename) && MessageBox.Show(this, $"Replace the existing database?\n{filename}",
                "Create SQLite", MessageBoxButtons.YesNo, MessageBoxIcon.Warning) != DialogResult.Yes) return;
            await CreateSqlite(filename, null);
        }

        public async Task CreateSqlite(string filename, string password)
        {
            var watch = new Stopwatch();
            SQLiteMigrationResult migration = null;
            await RunOperationAsync(() =>
            {
                watch.Start();
                using var access = DbAccessFactory.Create(AppState.ConnectionString, AppState.DatabaseType);
                Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(filename)));
                SqlServerToSQLite.dbs = dbs;
                dbs.ViewDefinitions = ((SqlServerDbAccess)access).ReadViewDefinitions();
                dbs.Procedures = ((SqlServerDbAccess)access).ReadProcedureDefinitions();
                migration = SqlServerToSQLite.CreateSQLiteDatabase(filename, password, true);
                SqlServerToSQLite.CopySqlServerRowsToSQLiteDB(access.ConnectionString, filename, password);
                watch.Stop();
            }, () =>
            {
                var viewErrors = migration.ViewErrors;
                string message = $"SQLite creation successful! Time: {watch.Elapsed.TotalSeconds:F1} seconds, Copy Rows: {SqlServerToSQLite.TotalCopyed} ";
                message += $"Views created: {dbs.ViewDefinitions.Count - viewErrors.Count}, failed: {viewErrors.Count}";
                message += $"; procedures fully converted: {dbs.Procedures.Count - migration.ProcedureErrors.Count}, partial/unsupported: {migration.ProcedureErrors.Count}";
                if (viewErrors.Count > 0 || migration.ProcedureErrors.Count > 0)
                {
                    message = "Migration completed with conversion errors. " + message;
                    MessageBox.Show(this, string.Join(Environment.NewLine, viewErrors.Concat(migration.ProcedureErrors)),
                        "Migration errors", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                }
                ShowMessage(message, EventArgs.Empty);
            }, "Processing...", reportSqliteProgress: true);
        }

        public Task CheckConnectionAsync()
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
            if (listBox1.Items.Count < 1)
            {
                MessageBox.Show("please choose a table !");
                return;
            }

            IList<string> SheetNames = new List<string>();

            foreach (var item in listBox1.Items)
            {
                SheetNames.Add(item.ToString());
            }

            string filename = FileDialogs.ShowSaveDialog(SheetNames[0], ".xlsx");

            if (filename != null)
            {
                await ExportTablesWithMiniExcelAsync(SheetNames, filename);

            }
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
            await ImportFolderAsync(tableName, textBox3.Text);
        }

        public async Task ImportFolderAsync(string DbTableName, string path)
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
            }, () => ShowMessage($"Import success, Time: {watch.Elapsed.TotalSeconds:F1} seconds", EventArgs.Empty),
                "Processing...");
        }

        private void buttonExAdd_Click(object sender, EventArgs e)
        {
            if (listBox1.Items.Contains(treeView1.SelectedNode.Text.ToString()))
            {
                return;
            }
            else
            {
                listBox1.Items.Add(treeView1.SelectedNode.Text.ToString());
            }
        }

        private void buttonExRemove_Click(object sender, EventArgs e)
        {
            if (listBox1.SelectedIndex < 0)
            {
                MessageBox.Show("Please Choose a Table！");
            }
            else
            {
                listBox1.Items.RemoveAt(listBox1.SelectedIndex);
            }
        }

        private void btnAddProce_Click(object sender, EventArgs e)
        {
            if (listBox2.Items.Contains(treeView2.SelectedNode.Text.ToString()))
            {
                return;
            }

            else
            {
                listBox2.Items.Add(treeView2.SelectedNode.Text.ToString());
            }
        }

        private void btnDeleteProc_Click(object sender, EventArgs e)
        {
            if (listBox2.SelectedIndex < 0)
            {
                MessageBox.Show("Pease Choose a procedure");
            }
            else
            {
                listBox2.Items.RemoveAt(listBox2.SelectedIndex);
            }
        }
    }
}
