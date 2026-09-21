using System;
using System.Linq;
using System.ServiceProcess;
using System.Threading.Tasks;
using System.Windows.Forms;
using DBUtil;

namespace DataPieDesktop
{
    public partial class DatabaseConnectionForm : Form
    {
        private static Main main;
        private readonly ConnectionStore connectionStore = new();
        private int selectedConnectionId;
        private bool operationRunning;

        public DatabaseConnectionForm()
        {
            InitializeComponent();
            savedConnectionComboBox.DropDownStyle = ComboBoxStyle.DropDownList;
            databaseTypeComboBox.DropDownStyle = ComboBoxStyle.DropDownList;
            authenticationModeComboBox.DropDownStyle = ComboBoxStyle.DropDownList;
            passwordTextBox.UseSystemPasswordChar = true;
        }

        private async Task RunActionAsync(Func<Task> action)
        {
            if (operationRunning) return;
            operationRunning = true;
            var controls = Controls.Cast<Control>().Select(control => (Control: control, control.Enabled)).ToArray();
            foreach (var item in controls) item.Control.Enabled = false;
            try { await action(); }
            catch (Exception ex)
            {
                if (!IsDisposed) MessageBox.Show(this, ex.Message, "Connection error", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
            finally
            {
                operationRunning = false;
                if (!IsDisposed)
                {
                    foreach (var item in controls) item.Control.Enabled = item.Enabled;
                    connectSavedConnectionButton.Enabled = savedConnectionComboBox.SelectedItem is Dbinfo;
                }
            }
        }

        private async void DatabaseConnectionForm_Load(object sender, EventArgs e)
        {
            databaseTypeComboBox.DataSource = new[] { "SQLSERVER", "SQLITE" };
            authenticationModeComboBox.SelectedIndex = 0;
            authenticationModeComboBox_SelectedIndexChanged(sender, e);
            await RunActionAsync(() => { LoadSavedConnections(); return Task.CompletedTask; });
        }

        private void LoadSavedConnections()
        {
            int? selectedId = (savedConnectionComboBox.SelectedItem as Dbinfo)?.Id;
            var records = connectionStore.Load();
            savedConnectionsGridView.DataSource = records;
            savedConnectionComboBox.DisplayMember = nameof(Dbinfo.Dbname);
            savedConnectionComboBox.ValueMember = nameof(Dbinfo.Id);
            savedConnectionComboBox.DataSource = records.ToList();
            if (selectedId.HasValue && records.Any(record => record.Id == selectedId))
                savedConnectionComboBox.SelectedValue = selectedId.Value;
            connectSavedConnectionButton.Enabled = records.Count > 0;
        }

        private void savedConnectionComboBox_SelectedIndexChanged(object sender, EventArgs e)
            => connectSavedConnectionButton.Enabled = savedConnectionComboBox.SelectedItem is Dbinfo;

        private void savedConnectionsGridView_RowHeaderMouseClick(object sender, DataGridViewCellMouseEventArgs e)
        {
            if (e.RowIndex < 0 || savedConnectionsGridView.Rows[e.RowIndex].DataBoundItem is not Dbinfo record) return;
            selectedConnectionId = record.Id;
            connectionNameTextBox.Text = record.Dbname;
            connectionStringTextBox.Text = record.ConnectionStrings;
            databaseTypeComboBox.Text = record.Dbtype;
        }

        private Dbinfo ReadEditedConnection() => new()
        {
            Id = selectedConnectionId, Dbname = connectionNameTextBox.Text.Trim(),
            ConnectionStrings = connectionStringTextBox.Text, Dbtype = databaseTypeComboBox.Text
        };

        private async Task EditConnectionAsync(Action edit)
        {
            await RunActionAsync(() =>
            {
                edit();
                LoadSavedConnections();
                connectionNameTextBox.Clear();
                connectionStringTextBox.Clear();
                selectedConnectionId = 0;
                return Task.CompletedTask;
            });
        }

        private async void addConnectionButton_Click(object sender, EventArgs e) => await EditConnectionAsync(() =>
        {
            var record = ReadEditedConnection();
            record.Id = 0;
            connectionStore.Save(record);
        });

        private async void updateConnectionButton_Click(object sender, EventArgs e) => await EditConnectionAsync(() =>
        {
            if (selectedConnectionId <= 0) throw new InvalidOperationException("Please select a record to update.");
            connectionStore.Save(ReadEditedConnection());
        });

        private async void deleteConnectionButton_Click(object sender, EventArgs e) => await EditConnectionAsync(() =>
        {
            if (selectedConnectionId <= 0) throw new InvalidOperationException("Please select a record to delete.");
            connectionStore.Delete(selectedConnectionId);
        });

        private static void TestConnection(Dbinfo record)
        {
            if (record == null || string.IsNullOrWhiteSpace(record.ConnectionStrings))
                throw new InvalidOperationException("Please select a database connection.");
            using var access = DbAccessFactory.Create(record.ConnectionStrings, record.Dbtype);
            access.conn.Open();
        }

        private async void testConnectionButton_Click(object sender, EventArgs e)
        {
            var record = ReadEditedConnection();
            await RunActionAsync(async () =>
            {
                await Task.Run(() => TestConnection(record));
                MessageBox.Show(this, "Test success!");
            });
        }

        private async Task ConnectAsync(Func<Dbinfo> readConnection, bool save)
        {
            await RunActionAsync(async () =>
            {
                var record = readConnection();
                await Task.Run(() => TestConnection(record));
                if (save) connectionStore.Save(record, onlyIfMissing: true);
                AppState.ConnectionString = record.ConnectionStrings;
                AppState.DatabaseType = record.Dbtype;
                AppState.DatabaseName = record.Dbname;
                if (main == null || main.IsDisposed)
                {
                    main = new Main();
                    main.Show();
                }
                else
                {
                    main.Show();
                    await main.LoadDatabaseSchemaAsync();
                }
                Hide();
            });
        }

        private async void connectSavedConnectionButton_Click(object sender, EventArgs e)
            => await ConnectAsync(() => savedConnectionComboBox.SelectedItem as Dbinfo, false);

        private DBConfig ReadSqlServerConfig(string database)
        {
            if (string.IsNullOrWhiteSpace(serverNameComboBox.Text)) throw new InvalidOperationException("Please enter a server.");
            return new DBConfig
            {
                ProviderName = "SQLSERVER", ServerName = serverNameComboBox.Text.Trim(), DataBase = database,
                ValidataType = authenticationModeComboBox.Text, UserName = userNameTextBox.Text, UserPwd = passwordTextBox.Text
            };
        }

        private async void loadDatabasesButton_Click(object sender, EventArgs e)
        {
            await RunActionAsync(async () =>
            {
                databaseNameComboBox.DataSource = null;
                databaseNameComboBox.Text = string.Empty;
                databaseNameComboBox.Enabled = false;
                string connection = ReadSqlServerConfig("master").GetSQLmasterConstring();
                var databases = await Task.Run(() =>
                {
                    using var access = DbAccessFactory.Create(connection, "SQLSERVER");
                    return access.GetDataBaseInfo();
                });
                databaseNameComboBox.DataSource = databases;
                databaseNameComboBox.Enabled = databases.Count > 0;
            });
        }

        private async void connectSqlServerButton_Click(object sender, EventArgs e) => await ConnectAsync(() =>
        {
            if (string.IsNullOrWhiteSpace(databaseNameComboBox.Text)) throw new InvalidOperationException("Please select a database.");
            return new Dbinfo { Dbname = databaseNameComboBox.Text, Dbtype = "SQLSERVER", ConnectionStrings = ReadSqlServerConfig(databaseNameComboBox.Text).GetConstring() };
        }, true);

        private void browseSqliteFileButton_Click(object sender, EventArgs e)
        {
            using var dialog = new OpenFileDialog { Filter = "SQLite|*.db", RestoreDirectory = true };
            if (dialog.ShowDialog(this) != DialogResult.OK) return;
            sqliteFilePathTextBox.Text = dialog.FileName;
            sqliteFilePathTextBox.ReadOnly = true;
        }

        private async void connectSqliteButton_Click(object sender, EventArgs e) => await ConnectAsync(() =>
        {
            if (string.IsNullOrWhiteSpace(sqliteFilePathTextBox.Text)) throw new InvalidOperationException("Please select a database.");
            var config = new DBConfig { ProviderName = "SQLITE", DataBase = sqliteFilePathTextBox.Text };
            return new Dbinfo { Dbname = sqliteFilePathTextBox.Text, Dbtype = "SQLITE", ConnectionStrings = config.GetConstring() };
        }, true);

        private void authenticationModeComboBox_SelectedIndexChanged(object sender, EventArgs e)
        {
            bool sqlAuthentication = authenticationModeComboBox.Text != "Windows";
            userNameTextBox.Enabled = sqlAuthentication;
            passwordTextBox.Enabled = sqlAuthentication;
        }

        private async void startLocalSqlServerButton_Click(object sender, EventArgs e) => await ChangeLocalServiceAsync(true);
        private async void stopLocalSqlServerButton_Click(object sender, EventArgs e) => await ChangeLocalServiceAsync(false);

        private async Task ChangeLocalServiceAsync(bool start)
        {
            await RunActionAsync(async () =>
            {
                await Task.Run(() =>
                {
                    using var service = new ServiceController("MSSQLSERVER", Environment.MachineName);
                    var target = start ? ServiceControllerStatus.Running : ServiceControllerStatus.Stopped;
                    if (service.Status == target) return;
                    if (start && service.Status != ServiceControllerStatus.StartPending) service.Start();
                    if (!start && service.Status != ServiceControllerStatus.StopPending) service.Stop();
                    service.WaitForStatus(target, TimeSpan.FromSeconds(30));
                });
                MessageBox.Show(this, start ? "Local SQL Server started." : "Local SQL Server stopped.");
            });
        }
    }
}
