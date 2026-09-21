
namespace DataPieDesktop
{
    partial class DatabaseConnectionForm
    {
        /// <summary>
        ///  Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        ///  Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        ///  Required method for Designer support - do not modify
        ///  the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(DatabaseConnectionForm));
            connectSavedConnectionButton = new System.Windows.Forms.Button();
            savedConnectionComboBox = new System.Windows.Forms.ComboBox();
            connectionTabControl = new System.Windows.Forms.TabControl();
            sqlServerTabPage = new System.Windows.Forms.TabPage();
            sqlServerConnectionGroupBox = new System.Windows.Forms.GroupBox();
            connectSqlServerButton = new System.Windows.Forms.Button();
            stopLocalSqlServerButton = new System.Windows.Forms.Button();
            passwordTextBox = new System.Windows.Forms.TextBox();
            startLocalSqlServerButton = new System.Windows.Forms.Button();
            loadDatabasesButton = new System.Windows.Forms.Button();
            userNameTextBox = new System.Windows.Forms.TextBox();
            databaseNameComboBox = new System.Windows.Forms.ComboBox();
            serverNameComboBox = new System.Windows.Forms.ComboBox();
            authenticationModeComboBox = new System.Windows.Forms.ComboBox();
            databaseNameLabel = new System.Windows.Forms.Label();
            passwordLabel = new System.Windows.Forms.Label();
            userNameLabel = new System.Windows.Forms.Label();
            serverNameLabel = new System.Windows.Forms.Label();
            authenticationModeLabel = new System.Windows.Forms.Label();
            sqliteTabPage = new System.Windows.Forms.TabPage();
            connectSqliteButton = new System.Windows.Forms.Button();
            browseSqliteFileButton = new System.Windows.Forms.Button();
            sqliteFilePathTextBox = new System.Windows.Forms.TextBox();
            sqliteFilePathLabel = new System.Windows.Forms.Label();
            savedConnectionsTabPage = new System.Windows.Forms.TabPage();
            connectionSettingsTabPage = new System.Windows.Forms.TabPage();
            testConnectionButton = new System.Windows.Forms.Button();
            savedConnectionsGridView = new System.Windows.Forms.DataGridView();
            databaseTypeComboBox = new System.Windows.Forms.ComboBox();
            connectionNameLabel = new System.Windows.Forms.Label();
            connectionNameTextBox = new System.Windows.Forms.TextBox();
            deleteConnectionButton = new System.Windows.Forms.Button();
            connectionStringTextBox = new System.Windows.Forms.TextBox();
            updateConnectionButton = new System.Windows.Forms.Button();
            databaseTypeLabel = new System.Windows.Forms.Label();
            addConnectionButton = new System.Windows.Forms.Button();
            connectionStringLabel = new System.Windows.Forms.Label();
            connectionTabControl.SuspendLayout();
            sqlServerTabPage.SuspendLayout();
            sqlServerConnectionGroupBox.SuspendLayout();
            sqliteTabPage.SuspendLayout();
            savedConnectionsTabPage.SuspendLayout();
            connectionSettingsTabPage.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)savedConnectionsGridView).BeginInit();
            SuspendLayout();
            //
            // connectSavedConnectionButton
            //
            connectSavedConnectionButton.Location = new System.Drawing.Point(584, 59);
            connectSavedConnectionButton.Name = "connectSavedConnectionButton";
            connectSavedConnectionButton.Size = new System.Drawing.Size(121, 39);
            connectSavedConnectionButton.TabIndex = 0;
            connectSavedConnectionButton.Text = "Connect";
            connectSavedConnectionButton.UseVisualStyleBackColor = true;
            connectSavedConnectionButton.Click += connectSavedConnectionButton_Click;
            //
            // savedConnectionComboBox
            //
            savedConnectionComboBox.FormattingEnabled = true;
            savedConnectionComboBox.ItemHeight = 20;
            savedConnectionComboBox.Location = new System.Drawing.Point(150, 62);
            savedConnectionComboBox.Name = "savedConnectionComboBox";
            savedConnectionComboBox.Size = new System.Drawing.Size(374, 28);
            savedConnectionComboBox.TabIndex = 1;
            savedConnectionComboBox.SelectedIndexChanged += savedConnectionComboBox_SelectedIndexChanged;
            //
            // connectionTabControl
            //
            connectionTabControl.Controls.Add(sqlServerTabPage);
            connectionTabControl.Controls.Add(sqliteTabPage);
            connectionTabControl.Controls.Add(savedConnectionsTabPage);
            connectionTabControl.Controls.Add(connectionSettingsTabPage);
            connectionTabControl.Location = new System.Drawing.Point(13, 26);
            connectionTabControl.Name = "connectionTabControl";
            connectionTabControl.SelectedIndex = 0;
            connectionTabControl.Size = new System.Drawing.Size(934, 502);
            connectionTabControl.TabIndex = 14;
            //
            // sqlServerTabPage
            //
            sqlServerTabPage.Controls.Add(sqlServerConnectionGroupBox);
            sqlServerTabPage.Location = new System.Drawing.Point(4, 29);
            sqlServerTabPage.Name = "sqlServerTabPage";
            sqlServerTabPage.Padding = new System.Windows.Forms.Padding(3);
            sqlServerTabPage.Size = new System.Drawing.Size(926, 469);
            sqlServerTabPage.TabIndex = 2;
            sqlServerTabPage.Text = "SqlServer";
            sqlServerTabPage.UseVisualStyleBackColor = true;
            //
            // sqlServerConnectionGroupBox
            //
            sqlServerConnectionGroupBox.Controls.Add(connectSqlServerButton);
            sqlServerConnectionGroupBox.Controls.Add(stopLocalSqlServerButton);
            sqlServerConnectionGroupBox.Controls.Add(passwordTextBox);
            sqlServerConnectionGroupBox.Controls.Add(startLocalSqlServerButton);
            sqlServerConnectionGroupBox.Controls.Add(loadDatabasesButton);
            sqlServerConnectionGroupBox.Controls.Add(userNameTextBox);
            sqlServerConnectionGroupBox.Controls.Add(databaseNameComboBox);
            sqlServerConnectionGroupBox.Controls.Add(serverNameComboBox);
            sqlServerConnectionGroupBox.Controls.Add(authenticationModeComboBox);
            sqlServerConnectionGroupBox.Controls.Add(databaseNameLabel);
            sqlServerConnectionGroupBox.Controls.Add(passwordLabel);
            sqlServerConnectionGroupBox.Controls.Add(userNameLabel);
            sqlServerConnectionGroupBox.Controls.Add(serverNameLabel);
            sqlServerConnectionGroupBox.Controls.Add(authenticationModeLabel);
            sqlServerConnectionGroupBox.Location = new System.Drawing.Point(0, 6);
            sqlServerConnectionGroupBox.Name = "sqlServerConnectionGroupBox";
            sqlServerConnectionGroupBox.Size = new System.Drawing.Size(905, 457);
            sqlServerConnectionGroupBox.TabIndex = 2;
            sqlServerConnectionGroupBox.TabStop = false;
            sqlServerConnectionGroupBox.Text = "SQL Server Connection";
            //
            // connectSqlServerButton
            //
            connectSqlServerButton.Location = new System.Drawing.Point(349, 288);
            connectSqlServerButton.Name = "connectSqlServerButton";
            connectSqlServerButton.Size = new System.Drawing.Size(133, 53);
            connectSqlServerButton.TabIndex = 4;
            connectSqlServerButton.Text = "Connect";
            connectSqlServerButton.UseVisualStyleBackColor = true;
            connectSqlServerButton.Click += connectSqlServerButton_Click;
            //
            // stopLocalSqlServerButton
            //
            stopLocalSqlServerButton.Location = new System.Drawing.Point(587, 321);
            stopLocalSqlServerButton.Name = "stopLocalSqlServerButton";
            stopLocalSqlServerButton.Size = new System.Drawing.Size(141, 42);
            stopLocalSqlServerButton.TabIndex = 1;
            stopLocalSqlServerButton.Text = "Stop  SQL Server";
            stopLocalSqlServerButton.UseVisualStyleBackColor = true;
            stopLocalSqlServerButton.Click += stopLocalSqlServerButton_Click;
            //
            // passwordTextBox
            //
            passwordTextBox.Location = new System.Drawing.Point(201, 182);
            passwordTextBox.Name = "passwordTextBox";
            passwordTextBox.Size = new System.Drawing.Size(281, 27);
            passwordTextBox.TabIndex = 9;
            //
            // startLocalSqlServerButton
            //
            startLocalSqlServerButton.Location = new System.Drawing.Point(587, 258);
            startLocalSqlServerButton.Name = "startLocalSqlServerButton";
            startLocalSqlServerButton.Size = new System.Drawing.Size(141, 48);
            startLocalSqlServerButton.TabIndex = 0;
            startLocalSqlServerButton.Text = "Start SQL Server";
            startLocalSqlServerButton.UseVisualStyleBackColor = true;
            startLocalSqlServerButton.Click += startLocalSqlServerButton_Click;
            //
            // loadDatabasesButton
            //
            loadDatabasesButton.Location = new System.Drawing.Point(192, 289);
            loadDatabasesButton.Name = "loadDatabasesButton";
            loadDatabasesButton.Size = new System.Drawing.Size(129, 52);
            loadDatabasesButton.TabIndex = 3;
            loadDatabasesButton.Text = "Load Databases";
            loadDatabasesButton.UseVisualStyleBackColor = true;
            loadDatabasesButton.Click += loadDatabasesButton_Click;
            //
            // userNameTextBox
            //
            userNameTextBox.Location = new System.Drawing.Point(201, 137);
            userNameTextBox.Name = "userNameTextBox";
            userNameTextBox.Size = new System.Drawing.Size(281, 27);
            userNameTextBox.TabIndex = 8;
            //
            // databaseNameComboBox
            //
            databaseNameComboBox.FormattingEnabled = true;
            databaseNameComboBox.Location = new System.Drawing.Point(201, 229);
            databaseNameComboBox.Name = "databaseNameComboBox";
            databaseNameComboBox.Size = new System.Drawing.Size(280, 28);
            databaseNameComboBox.TabIndex = 7;
            //
            // serverNameComboBox
            //
            serverNameComboBox.FormattingEnabled = true;
            serverNameComboBox.Location = new System.Drawing.Point(201, 87);
            serverNameComboBox.Name = "serverNameComboBox";
            serverNameComboBox.Size = new System.Drawing.Size(280, 28);
            serverNameComboBox.TabIndex = 6;
            serverNameComboBox.Text = "(local)";
            //
            // authenticationModeComboBox
            //
            authenticationModeComboBox.FormattingEnabled = true;
            authenticationModeComboBox.Items.AddRange(new object[] { "Windows", "SQL Server" });
            authenticationModeComboBox.Location = new System.Drawing.Point(201, 40);
            authenticationModeComboBox.Name = "authenticationModeComboBox";
            authenticationModeComboBox.Size = new System.Drawing.Size(280, 28);
            authenticationModeComboBox.TabIndex = 5;
            authenticationModeComboBox.Text = "Windows";
            authenticationModeComboBox.SelectedIndexChanged += authenticationModeComboBox_SelectedIndexChanged;
            //
            // databaseNameLabel
            //
            databaseNameLabel.AutoSize = true;
            databaseNameLabel.Location = new System.Drawing.Point(69, 237);
            databaseNameLabel.Name = "databaseNameLabel";
            databaseNameLabel.Size = new System.Drawing.Size(69, 20);
            databaseNameLabel.TabIndex = 4;
            databaseNameLabel.Text = "DBname";
            //
            // passwordLabel
            //
            passwordLabel.AutoSize = true;
            passwordLabel.Location = new System.Drawing.Point(69, 182);
            passwordLabel.Name = "passwordLabel";
            passwordLabel.Size = new System.Drawing.Size(78, 20);
            passwordLabel.TabIndex = 3;
            passwordLabel.Text = "Password";
            //
            // userNameLabel
            //
            userNameLabel.AutoSize = true;
            userNameLabel.Location = new System.Drawing.Point(69, 137);
            userNameLabel.Name = "userNameLabel";
            userNameLabel.Size = new System.Drawing.Size(85, 20);
            userNameLabel.TabIndex = 2;
            userNameLabel.Text = "UserName";
            //
            // serverNameLabel
            //
            serverNameLabel.AutoSize = true;
            serverNameLabel.Location = new System.Drawing.Point(69, 87);
            serverNameLabel.Name = "serverNameLabel";
            serverNameLabel.Size = new System.Drawing.Size(56, 20);
            serverNameLabel.TabIndex = 1;
            serverNameLabel.Text = "Server";
            //
            // authenticationModeLabel
            //
            authenticationModeLabel.AutoSize = true;
            authenticationModeLabel.Location = new System.Drawing.Point(69, 43);
            authenticationModeLabel.Name = "authenticationModeLabel";
            authenticationModeLabel.Size = new System.Drawing.Size(132, 20);
            authenticationModeLabel.TabIndex = 0;
            authenticationModeLabel.Text = "Authentication：";
            //
            // sqliteTabPage
            //
            sqliteTabPage.Controls.Add(connectSqliteButton);
            sqliteTabPage.Controls.Add(browseSqliteFileButton);
            sqliteTabPage.Controls.Add(sqliteFilePathTextBox);
            sqliteTabPage.Controls.Add(sqliteFilePathLabel);
            sqliteTabPage.Location = new System.Drawing.Point(4, 29);
            sqliteTabPage.Name = "sqliteTabPage";
            sqliteTabPage.Padding = new System.Windows.Forms.Padding(3);
            sqliteTabPage.Size = new System.Drawing.Size(926, 469);
            sqliteTabPage.TabIndex = 3;
            sqliteTabPage.Text = "Sqlite";
            sqliteTabPage.UseVisualStyleBackColor = true;
            //
            // connectSqliteButton
            //
            connectSqliteButton.Location = new System.Drawing.Point(515, 194);
            connectSqliteButton.Name = "connectSqliteButton";
            connectSqliteButton.Size = new System.Drawing.Size(138, 46);
            connectSqliteButton.TabIndex = 3;
            connectSqliteButton.Text = "Connect";
            connectSqliteButton.UseVisualStyleBackColor = true;
            connectSqliteButton.Click += connectSqliteButton_Click;
            //
            // browseSqliteFileButton
            //
            browseSqliteFileButton.Location = new System.Drawing.Point(336, 194);
            browseSqliteFileButton.Name = "browseSqliteFileButton";
            browseSqliteFileButton.Size = new System.Drawing.Size(124, 46);
            browseSqliteFileButton.TabIndex = 2;
            browseSqliteFileButton.Text = "Browse...";
            browseSqliteFileButton.UseVisualStyleBackColor = true;
            browseSqliteFileButton.Click += browseSqliteFileButton_Click;
            //
            // sqliteFilePathTextBox
            //
            sqliteFilePathTextBox.Location = new System.Drawing.Point(242, 89);
            sqliteFilePathTextBox.Name = "sqliteFilePathTextBox";
            sqliteFilePathTextBox.Size = new System.Drawing.Size(470, 27);
            sqliteFilePathTextBox.TabIndex = 1;
            //
            // sqliteFilePathLabel
            //
            sqliteFilePathLabel.AutoSize = true;
            sqliteFilePathLabel.Location = new System.Drawing.Point(132, 96);
            sqliteFilePathLabel.Name = "sqliteFilePathLabel";
            sqliteFilePathLabel.Size = new System.Drawing.Size(75, 20);
            sqliteFilePathLabel.TabIndex = 0;
            sqliteFilePathLabel.Text = "File path:";
            //
            // savedConnectionsTabPage
            //
            savedConnectionsTabPage.Controls.Add(savedConnectionComboBox);
            savedConnectionsTabPage.Controls.Add(connectSavedConnectionButton);
            savedConnectionsTabPage.Location = new System.Drawing.Point(4, 29);
            savedConnectionsTabPage.Name = "savedConnectionsTabPage";
            savedConnectionsTabPage.Padding = new System.Windows.Forms.Padding(3);
            savedConnectionsTabPage.Size = new System.Drawing.Size(926, 469);
            savedConnectionsTabPage.TabIndex = 0;
            savedConnectionsTabPage.Text = "All DataBase";
            savedConnectionsTabPage.UseVisualStyleBackColor = true;
            //
            // connectionSettingsTabPage
            //
            connectionSettingsTabPage.Controls.Add(testConnectionButton);
            connectionSettingsTabPage.Controls.Add(savedConnectionsGridView);
            connectionSettingsTabPage.Controls.Add(databaseTypeComboBox);
            connectionSettingsTabPage.Controls.Add(connectionNameLabel);
            connectionSettingsTabPage.Controls.Add(connectionNameTextBox);
            connectionSettingsTabPage.Controls.Add(deleteConnectionButton);
            connectionSettingsTabPage.Controls.Add(connectionStringTextBox);
            connectionSettingsTabPage.Controls.Add(updateConnectionButton);
            connectionSettingsTabPage.Controls.Add(databaseTypeLabel);
            connectionSettingsTabPage.Controls.Add(addConnectionButton);
            connectionSettingsTabPage.Controls.Add(connectionStringLabel);
            connectionSettingsTabPage.Location = new System.Drawing.Point(4, 29);
            connectionSettingsTabPage.Name = "connectionSettingsTabPage";
            connectionSettingsTabPage.Padding = new System.Windows.Forms.Padding(3);
            connectionSettingsTabPage.Size = new System.Drawing.Size(926, 469);
            connectionSettingsTabPage.TabIndex = 1;
            connectionSettingsTabPage.Text = "Config";
            connectionSettingsTabPage.UseVisualStyleBackColor = true;
            //
            // testConnectionButton
            //
            testConnectionButton.Location = new System.Drawing.Point(49, 122);
            testConnectionButton.Name = "testConnectionButton";
            testConnectionButton.Size = new System.Drawing.Size(106, 29);
            testConnectionButton.TabIndex = 25;
            testConnectionButton.Text = "Test";
            testConnectionButton.UseVisualStyleBackColor = true;
            testConnectionButton.Click += testConnectionButton_Click;
            //
            // savedConnectionsGridView
            //
            savedConnectionsGridView.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            savedConnectionsGridView.Location = new System.Drawing.Point(49, 164);
            savedConnectionsGridView.Name = "savedConnectionsGridView";
            savedConnectionsGridView.RowHeadersWidth = 51;
            savedConnectionsGridView.Size = new System.Drawing.Size(856, 284);
            savedConnectionsGridView.TabIndex = 15;
            savedConnectionsGridView.RowHeaderMouseClick += savedConnectionsGridView_RowHeaderMouseClick;
            //
            // databaseTypeComboBox
            //
            databaseTypeComboBox.FormattingEnabled = true;
            databaseTypeComboBox.Location = new System.Drawing.Point(574, 34);
            databaseTypeComboBox.Name = "databaseTypeComboBox";
            databaseTypeComboBox.Size = new System.Drawing.Size(266, 28);
            databaseTypeComboBox.TabIndex = 24;
            //
            // connectionNameLabel
            //
            connectionNameLabel.AutoSize = true;
            connectionNameLabel.Location = new System.Drawing.Point(49, 41);
            connectionNameLabel.Name = "connectionNameLabel";
            connectionNameLabel.Size = new System.Drawing.Size(70, 20);
            connectionNameLabel.TabIndex = 17;
            connectionNameLabel.Text = "Dbname";
            //
            // connectionNameTextBox
            //
            connectionNameTextBox.Location = new System.Drawing.Point(131, 34);
            connectionNameTextBox.Name = "connectionNameTextBox";
            connectionNameTextBox.Size = new System.Drawing.Size(328, 27);
            connectionNameTextBox.TabIndex = 14;
            //
            // deleteConnectionButton
            //
            deleteConnectionButton.Location = new System.Drawing.Point(744, 122);
            deleteConnectionButton.Name = "deleteConnectionButton";
            deleteConnectionButton.Size = new System.Drawing.Size(106, 29);
            deleteConnectionButton.TabIndex = 22;
            deleteConnectionButton.Text = "Delete";
            deleteConnectionButton.UseVisualStyleBackColor = true;
            deleteConnectionButton.Click += deleteConnectionButton_Click;
            //
            // connectionStringTextBox
            //
            connectionStringTextBox.Location = new System.Drawing.Point(218, 77);
            connectionStringTextBox.Name = "connectionStringTextBox";
            connectionStringTextBox.Size = new System.Drawing.Size(621, 27);
            connectionStringTextBox.TabIndex = 16;
            //
            // updateConnectionButton
            //
            updateConnectionButton.Location = new System.Drawing.Point(505, 122);
            updateConnectionButton.Name = "updateConnectionButton";
            updateConnectionButton.Size = new System.Drawing.Size(106, 29);
            updateConnectionButton.TabIndex = 21;
            updateConnectionButton.Text = "Update";
            updateConnectionButton.UseVisualStyleBackColor = true;
            updateConnectionButton.Click += updateConnectionButton_Click;
            //
            // databaseTypeLabel
            //
            databaseTypeLabel.AutoSize = true;
            databaseTypeLabel.Location = new System.Drawing.Point(492, 41);
            databaseTypeLabel.Name = "databaseTypeLabel";
            databaseTypeLabel.Size = new System.Drawing.Size(63, 20);
            databaseTypeLabel.TabIndex = 18;
            databaseTypeLabel.Text = "Dbtype";
            //
            // addConnectionButton
            //
            addConnectionButton.Location = new System.Drawing.Point(268, 122);
            addConnectionButton.Name = "addConnectionButton";
            addConnectionButton.Size = new System.Drawing.Size(106, 29);
            addConnectionButton.TabIndex = 20;
            addConnectionButton.Text = "Add";
            addConnectionButton.UseVisualStyleBackColor = true;
            addConnectionButton.Click += addConnectionButton_Click;
            //
            // connectionStringLabel
            //
            connectionStringLabel.AutoSize = true;
            connectionStringLabel.Location = new System.Drawing.Point(49, 82);
            connectionStringLabel.Name = "connectionStringLabel";
            connectionStringLabel.Size = new System.Drawing.Size(144, 20);
            connectionStringLabel.TabIndex = 19;
            connectionStringLabel.Text = "ConnectionStrings";
            //
            // DatabaseConnectionForm
            //
            AutoScaleDimensions = new System.Drawing.SizeF(9F, 20F);
            AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            ClientSize = new System.Drawing.Size(988, 546);
            Controls.Add(connectionTabControl);
            Icon = (System.Drawing.Icon)resources.GetObject("$this.Icon");
            Name = "DatabaseConnectionForm";
            Text = "DataPie V2026.09";
            Load += DatabaseConnectionForm_Load;
            connectionTabControl.ResumeLayout(false);
            sqlServerTabPage.ResumeLayout(false);
            sqlServerConnectionGroupBox.ResumeLayout(false);
            sqlServerConnectionGroupBox.PerformLayout();
            sqliteTabPage.ResumeLayout(false);
            sqliteTabPage.PerformLayout();
            savedConnectionsTabPage.ResumeLayout(false);
            connectionSettingsTabPage.ResumeLayout(false);
            connectionSettingsTabPage.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)savedConnectionsGridView).EndInit();
            ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Button connectSavedConnectionButton;
        private System.Windows.Forms.ComboBox savedConnectionComboBox;
        private System.Windows.Forms.TabControl connectionTabControl;
        private System.Windows.Forms.TabPage savedConnectionsTabPage;
        private System.Windows.Forms.TabPage connectionSettingsTabPage;
        private System.Windows.Forms.DataGridView savedConnectionsGridView;
        private System.Windows.Forms.ComboBox databaseTypeComboBox;
        private System.Windows.Forms.Label connectionNameLabel;
        private System.Windows.Forms.TextBox connectionNameTextBox;
        private System.Windows.Forms.Button deleteConnectionButton;
        private System.Windows.Forms.TextBox connectionStringTextBox;
        private System.Windows.Forms.Button updateConnectionButton;
        private System.Windows.Forms.Label databaseTypeLabel;
        private System.Windows.Forms.Button addConnectionButton;
        private System.Windows.Forms.Label connectionStringLabel;
        private System.Windows.Forms.TabPage sqlServerTabPage;
        private System.Windows.Forms.Button stopLocalSqlServerButton;
        private System.Windows.Forms.Button startLocalSqlServerButton;
        private System.Windows.Forms.Button connectSqlServerButton;
        private System.Windows.Forms.Button loadDatabasesButton;
        private System.Windows.Forms.GroupBox sqlServerConnectionGroupBox;
        private System.Windows.Forms.TextBox passwordTextBox;
        private System.Windows.Forms.TextBox userNameTextBox;
        private System.Windows.Forms.ComboBox databaseNameComboBox;
        private System.Windows.Forms.ComboBox serverNameComboBox;
        private System.Windows.Forms.ComboBox authenticationModeComboBox;
        private System.Windows.Forms.Label databaseNameLabel;
        private System.Windows.Forms.Label passwordLabel;
        private System.Windows.Forms.Label userNameLabel;
        private System.Windows.Forms.Label serverNameLabel;
        private System.Windows.Forms.Label authenticationModeLabel;
        private System.Windows.Forms.TabPage sqliteTabPage;
        private System.Windows.Forms.Button connectSqliteButton;
        private System.Windows.Forms.Button browseSqliteFileButton;
        private System.Windows.Forms.TextBox sqliteFilePathTextBox;
        private System.Windows.Forms.Label sqliteFilePathLabel;
        private System.Windows.Forms.Button testConnectionButton;
    }
}

