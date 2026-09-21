
namespace DataPieDesktop
{
    partial class Main
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
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
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(Main));
            mainTabControl = new System.Windows.Forms.TabControl();
            importTabPage = new System.Windows.Forms.TabPage();
            importFolderGroupBox = new System.Windows.Forms.GroupBox();
            importFolderButton = new System.Windows.Forms.Button();
            importFolderPathTextBox = new System.Windows.Forms.TextBox();
            browseImportFolderButton = new System.Windows.Forms.Button();
            importFileGroupBox = new System.Windows.Forms.GroupBox();
            importFileButton = new System.Windows.Forms.Button();
            importFilePathTextBox = new System.Windows.Forms.TextBox();
            browseImportFileButton = new System.Windows.Forms.Button();
            importTableGroupBox = new System.Windows.Forms.GroupBox();
            importTableLabel = new System.Windows.Forms.Label();
            clearTableDataButton = new System.Windows.Forms.Button();
            importTableComboBox = new System.Windows.Forms.ComboBox();
            exportTemplateButton = new System.Windows.Forms.Button();
            exportTabPage = new System.Windows.Forms.TabPage();
            exportSelectionGroupBox = new System.Windows.Forms.GroupBox();
            removeExportItemButton = new System.Windows.Forms.Button();
            addExportItemButton = new System.Windows.Forms.Button();
            clearExportListButton = new System.Windows.Forms.Button();
            exportObjectsTreeView = new System.Windows.Forms.TreeView();
            selectedExportObjectsListBox = new System.Windows.Forms.ListBox();
            exportActionsGroupBox = new System.Windows.Forms.GroupBox();
            exportSheetsWithMiniExcelButton = new System.Windows.Forms.Button();
            exportTablesToCsvButton = new System.Windows.Forms.Button();
            exportSheetsWithEpplusButton = new System.Windows.Forms.Button();
            queryTabPage = new System.Windows.Forms.TabPage();
            sqlEditorRichTextBox = new System.Windows.Forms.RichTextBox();
            sqlActionsGroupBox = new System.Windows.Forms.GroupBox();
            generateDeleteSqlButton = new System.Windows.Forms.Button();
            generateUpdateSqlButton = new System.Windows.Forms.Button();
            executeSqlButton = new System.Windows.Forms.Button();
            queryResultsGridView = new System.Windows.Forms.DataGridView();
            queryActionsGroupBox = new System.Windows.Forms.GroupBox();
            exportTableToExcelButton = new System.Windows.Forms.Button();
            generateSelectSqlButton = new System.Windows.Forms.Button();
            exportTableToCsvButton = new System.Windows.Forms.Button();
            previewQueryButton = new System.Windows.Forms.Button();
            exportQueryToExcelButton = new System.Windows.Forms.Button();
            queryTableLabel = new System.Windows.Forms.Label();
            queryTableComboBox = new System.Windows.Forms.ComboBox();
            proceduresTabPage = new System.Windows.Forms.TabPage();
            procedureExecutionGroupBox = new System.Windows.Forms.GroupBox();
            removeProcedureButton = new System.Windows.Forms.Button();
            addProcedureButton = new System.Windows.Forms.Button();
            selectedProceduresListBox = new System.Windows.Forms.ListBox();
            executeProceduresButton = new System.Windows.Forms.Button();
            availableProceduresTreeView = new System.Windows.Forms.TreeView();
            sqliteMigrationTabPage = new System.Windows.Forms.TabPage();
            refreshSqliteProgressButton = new System.Windows.Forms.Button();
            cancelSqliteExportButton = new System.Windows.Forms.Button();
            sqliteOutputFolderTextBox = new System.Windows.Forms.TextBox();
            browseSqliteOutputFolderButton = new System.Windows.Forms.Button();
            createSqliteButton = new System.Windows.Forms.Button();
            mainToolStrip = new System.Windows.Forms.ToolStrip();
            databaseConnectionToolStripButton = new System.Windows.Forms.ToolStripButton();
            connectionToolStripSeparator = new System.Windows.Forms.ToolStripSeparator();
            exitToolStripButton = new System.Windows.Forms.ToolStripButton();
            aboutToolStripSeparator = new System.Windows.Forms.ToolStripSeparator();
            aboutToolStripButton = new System.Windows.Forms.ToolStripButton();
            mainStatusStrip = new System.Windows.Forms.StatusStrip();
            operationMessageStatusLabel = new WrappingStatusLabel();
            operationDetailsStatusLabel = new WrappingStatusLabel();
            mainTabControl.SuspendLayout();
            importTabPage.SuspendLayout();
            importFolderGroupBox.SuspendLayout();
            importFileGroupBox.SuspendLayout();
            importTableGroupBox.SuspendLayout();
            exportTabPage.SuspendLayout();
            exportSelectionGroupBox.SuspendLayout();
            exportActionsGroupBox.SuspendLayout();
            queryTabPage.SuspendLayout();
            sqlActionsGroupBox.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)queryResultsGridView).BeginInit();
            queryActionsGroupBox.SuspendLayout();
            proceduresTabPage.SuspendLayout();
            procedureExecutionGroupBox.SuspendLayout();
            sqliteMigrationTabPage.SuspendLayout();
            mainToolStrip.SuspendLayout();
            mainStatusStrip.SuspendLayout();
            SuspendLayout();
            //
            // mainTabControl
            //
            mainTabControl.Controls.Add(importTabPage);
            mainTabControl.Controls.Add(exportTabPage);
            mainTabControl.Controls.Add(queryTabPage);
            mainTabControl.Controls.Add(proceduresTabPage);
            mainTabControl.Controls.Add(sqliteMigrationTabPage);
            mainTabControl.Location = new System.Drawing.Point(11, 43);
            mainTabControl.Name = "mainTabControl";
            mainTabControl.SelectedIndex = 0;
            mainTabControl.Size = new System.Drawing.Size(1040, 603);
            mainTabControl.TabIndex = 0;
            //
            // importTabPage
            //
            importTabPage.Controls.Add(importFolderGroupBox);
            importTabPage.Controls.Add(importFileGroupBox);
            importTabPage.Controls.Add(importTableGroupBox);
            importTabPage.Location = new System.Drawing.Point(4, 29);
            importTabPage.Name = "importTabPage";
            importTabPage.Padding = new System.Windows.Forms.Padding(3);
            importTabPage.Size = new System.Drawing.Size(1032, 570);
            importTabPage.TabIndex = 0;
            importTabPage.Text = "Input";
            importTabPage.UseVisualStyleBackColor = true;
            //
            // importFolderGroupBox
            //
            importFolderGroupBox.Controls.Add(importFolderButton);
            importFolderGroupBox.Controls.Add(importFolderPathTextBox);
            importFolderGroupBox.Controls.Add(browseImportFolderButton);
            importFolderGroupBox.Location = new System.Drawing.Point(20, 347);
            importFolderGroupBox.Name = "importFolderGroupBox";
            importFolderGroupBox.Size = new System.Drawing.Size(997, 153);
            importFolderGroupBox.TabIndex = 6;
            importFolderGroupBox.TabStop = false;
            importFolderGroupBox.Text = "Excel/Csv folder Import";
            //
            // importFolderButton
            //
            importFolderButton.Location = new System.Drawing.Point(731, 47);
            importFolderButton.Name = "importFolderButton";
            importFolderButton.Size = new System.Drawing.Size(165, 50);
            importFolderButton.TabIndex = 2;
            importFolderButton.Text = "Import";
            importFolderButton.UseVisualStyleBackColor = true;
            importFolderButton.Click += importFolderButton_Click;
            //
            // importFolderPathTextBox
            //
            importFolderPathTextBox.Location = new System.Drawing.Point(227, 65);
            importFolderPathTextBox.Name = "importFolderPathTextBox";
            importFolderPathTextBox.Size = new System.Drawing.Size(389, 27);
            importFolderPathTextBox.TabIndex = 1;
            //
            // browseImportFolderButton
            //
            browseImportFolderButton.Location = new System.Drawing.Point(12, 60);
            browseImportFolderButton.Name = "browseImportFolderButton";
            browseImportFolderButton.Size = new System.Drawing.Size(168, 37);
            browseImportFolderButton.TabIndex = 0;
            browseImportFolderButton.Text = "Browse the folder..";
            browseImportFolderButton.UseVisualStyleBackColor = true;
            browseImportFolderButton.Click += browseImportFolderButton_Click;
            //
            // importFileGroupBox
            //
            importFileGroupBox.Controls.Add(importFileButton);
            importFileGroupBox.Controls.Add(importFilePathTextBox);
            importFileGroupBox.Controls.Add(browseImportFileButton);
            importFileGroupBox.Location = new System.Drawing.Point(20, 175);
            importFileGroupBox.Name = "importFileGroupBox";
            importFileGroupBox.Size = new System.Drawing.Size(997, 153);
            importFileGroupBox.TabIndex = 5;
            importFileGroupBox.TabStop = false;
            importFileGroupBox.Text = "Excel/Csv/Sqlite file Import";
            //
            // importFileButton
            //
            importFileButton.Location = new System.Drawing.Point(726, 47);
            importFileButton.Name = "importFileButton";
            importFileButton.Size = new System.Drawing.Size(165, 50);
            importFileButton.TabIndex = 2;
            importFileButton.Text = "Import";
            importFileButton.UseVisualStyleBackColor = true;
            importFileButton.Click += importFileButton_Click;
            //
            // importFilePathTextBox
            //
            importFilePathTextBox.Location = new System.Drawing.Point(220, 65);
            importFilePathTextBox.Name = "importFilePathTextBox";
            importFilePathTextBox.Size = new System.Drawing.Size(389, 27);
            importFilePathTextBox.TabIndex = 1;
            //
            // browseImportFileButton
            //
            browseImportFileButton.Location = new System.Drawing.Point(7, 60);
            browseImportFileButton.Name = "browseImportFileButton";
            browseImportFileButton.Size = new System.Drawing.Size(168, 37);
            browseImportFileButton.TabIndex = 0;
            browseImportFileButton.Text = "Browse the file...";
            browseImportFileButton.UseVisualStyleBackColor = true;
            browseImportFileButton.Click += browseImportFileButton_Click;
            //
            // importTableGroupBox
            //
            importTableGroupBox.Controls.Add(importTableLabel);
            importTableGroupBox.Controls.Add(clearTableDataButton);
            importTableGroupBox.Controls.Add(importTableComboBox);
            importTableGroupBox.Controls.Add(exportTemplateButton);
            importTableGroupBox.Location = new System.Drawing.Point(20, 20);
            importTableGroupBox.Name = "importTableGroupBox";
            importTableGroupBox.Size = new System.Drawing.Size(997, 137);
            importTableGroupBox.TabIndex = 4;
            importTableGroupBox.TabStop = false;
            importTableGroupBox.Text = " Table select";
            //
            // importTableLabel
            //
            importTableLabel.AutoSize = true;
            importTableLabel.Location = new System.Drawing.Point(18, 33);
            importTableLabel.Name = "importTableLabel";
            importTableLabel.Size = new System.Drawing.Size(169, 20);
            importTableLabel.TabIndex = 2;
            importTableLabel.Text = "please choose table：";
            //
            // clearTableDataButton
            //
            clearTableDataButton.Location = new System.Drawing.Point(726, 75);
            clearTableDataButton.Name = "clearTableDataButton";
            clearTableDataButton.Size = new System.Drawing.Size(169, 43);
            clearTableDataButton.TabIndex = 3;
            clearTableDataButton.Text = "Delete";
            clearTableDataButton.UseVisualStyleBackColor = true;
            clearTableDataButton.Click += clearTableDataButton_Click;
            //
            // importTableComboBox
            //
            importTableComboBox.FormattingEnabled = true;
            importTableComboBox.Location = new System.Drawing.Point(220, 27);
            importTableComboBox.Name = "importTableComboBox";
            importTableComboBox.Size = new System.Drawing.Size(389, 28);
            importTableComboBox.TabIndex = 0;
            //
            // exportTemplateButton
            //
            exportTemplateButton.Location = new System.Drawing.Point(722, 15);
            exportTemplateButton.Name = "exportTemplateButton";
            exportTemplateButton.Size = new System.Drawing.Size(169, 48);
            exportTemplateButton.TabIndex = 1;
            exportTemplateButton.Text = "Export Template";
            exportTemplateButton.UseVisualStyleBackColor = true;
            exportTemplateButton.Click += exportTemplateButton_Click;
            //
            // exportTabPage
            //
            exportTabPage.Controls.Add(exportSelectionGroupBox);
            exportTabPage.Controls.Add(exportActionsGroupBox);
            exportTabPage.Location = new System.Drawing.Point(4, 29);
            exportTabPage.Name = "exportTabPage";
            exportTabPage.Padding = new System.Windows.Forms.Padding(3);
            exportTabPage.Size = new System.Drawing.Size(1032, 570);
            exportTabPage.TabIndex = 1;
            exportTabPage.Text = "Output";
            exportTabPage.UseVisualStyleBackColor = true;
            //
            // exportSelectionGroupBox
            //
            exportSelectionGroupBox.Controls.Add(removeExportItemButton);
            exportSelectionGroupBox.Controls.Add(addExportItemButton);
            exportSelectionGroupBox.Controls.Add(clearExportListButton);
            exportSelectionGroupBox.Controls.Add(exportObjectsTreeView);
            exportSelectionGroupBox.Controls.Add(selectedExportObjectsListBox);
            exportSelectionGroupBox.Location = new System.Drawing.Point(29, 13);
            exportSelectionGroupBox.Name = "exportSelectionGroupBox";
            exportSelectionGroupBox.Size = new System.Drawing.Size(1004, 462);
            exportSelectionGroupBox.TabIndex = 6;
            exportSelectionGroupBox.TabStop = false;
            exportSelectionGroupBox.Text = "Choose Tables to Export";
            //
            // removeExportItemButton
            //
            removeExportItemButton.Location = new System.Drawing.Point(435, 158);
            removeExportItemButton.Margin = new System.Windows.Forms.Padding(2);
            removeExportItemButton.Name = "removeExportItemButton";
            removeExportItemButton.Size = new System.Drawing.Size(142, 37);
            removeExportItemButton.TabIndex = 8;
            removeExportItemButton.Text = "Remove a Table";
            removeExportItemButton.UseVisualStyleBackColor = true;
            removeExportItemButton.Click += removeExportItemButton_Click;
            //
            // addExportItemButton
            //
            addExportItemButton.Location = new System.Drawing.Point(435, 91);
            addExportItemButton.Margin = new System.Windows.Forms.Padding(2);
            addExportItemButton.Name = "addExportItemButton";
            addExportItemButton.Size = new System.Drawing.Size(142, 38);
            addExportItemButton.TabIndex = 7;
            addExportItemButton.Text = "Add a Table";
            addExportItemButton.UseVisualStyleBackColor = true;
            addExportItemButton.Click += addExportItemButton_Click;
            //
            // clearExportListButton
            //
            clearExportListButton.Location = new System.Drawing.Point(435, 293);
            clearExportListButton.Name = "clearExportListButton";
            clearExportListButton.Size = new System.Drawing.Size(142, 37);
            clearExportListButton.TabIndex = 6;
            clearExportListButton.Text = "Clear All Tables";
            clearExportListButton.UseVisualStyleBackColor = true;
            clearExportListButton.Click += clearExportListButton_Click;
            //
            // exportObjectsTreeView
            //
            exportObjectsTreeView.Location = new System.Drawing.Point(61, 37);
            exportObjectsTreeView.Name = "exportObjectsTreeView";
            exportObjectsTreeView.Size = new System.Drawing.Size(353, 421);
            exportObjectsTreeView.TabIndex = 0;
            exportObjectsTreeView.NodeMouseDoubleClick += exportObjectsTreeView_NodeMouseDoubleClick;
            //
            // selectedExportObjectsListBox
            //
            selectedExportObjectsListBox.FormattingEnabled = true;
            selectedExportObjectsListBox.Location = new System.Drawing.Point(614, 37);
            selectedExportObjectsListBox.Name = "selectedExportObjectsListBox";
            selectedExportObjectsListBox.Size = new System.Drawing.Size(335, 424);
            selectedExportObjectsListBox.TabIndex = 3;
            selectedExportObjectsListBox.DoubleClick += selectedExportObjectsListBox_DoubleClick;
            //
            // exportActionsGroupBox
            //
            exportActionsGroupBox.Controls.Add(exportSheetsWithMiniExcelButton);
            exportActionsGroupBox.Controls.Add(exportTablesToCsvButton);
            exportActionsGroupBox.Controls.Add(exportSheetsWithEpplusButton);
            exportActionsGroupBox.Location = new System.Drawing.Point(29, 482);
            exportActionsGroupBox.Name = "exportActionsGroupBox";
            exportActionsGroupBox.Size = new System.Drawing.Size(1004, 82);
            exportActionsGroupBox.TabIndex = 5;
            exportActionsGroupBox.TabStop = false;
            exportActionsGroupBox.Text = "Export";
            //
            // exportSheetsWithMiniExcelButton
            //
            exportSheetsWithMiniExcelButton.Location = new System.Drawing.Point(430, 27);
            exportSheetsWithMiniExcelButton.Margin = new System.Windows.Forms.Padding(2);
            exportSheetsWithMiniExcelButton.Name = "exportSheetsWithMiniExcelButton";
            exportSheetsWithMiniExcelButton.Size = new System.Drawing.Size(187, 50);
            exportSheetsWithMiniExcelButton.TabIndex = 6;
            exportSheetsWithMiniExcelButton.Text = "MiniExcel(Fast) ";
            exportSheetsWithMiniExcelButton.UseVisualStyleBackColor = true;
            exportSheetsWithMiniExcelButton.Click += exportSheetsWithMiniExcelButton_Click;
            //
            // exportTablesToCsvButton
            //
            exportTablesToCsvButton.Location = new System.Drawing.Point(660, 27);
            exportTablesToCsvButton.Name = "exportTablesToCsvButton";
            exportTablesToCsvButton.Size = new System.Drawing.Size(133, 50);
            exportTablesToCsvButton.TabIndex = 5;
            exportTablesToCsvButton.Text = "Export CSV";
            exportTablesToCsvButton.UseVisualStyleBackColor = true;
            exportTablesToCsvButton.Click += exportTablesToCsvButton_Click;
            //
            // exportSheetsWithEpplusButton
            //
            exportSheetsWithEpplusButton.Location = new System.Drawing.Point(822, 27);
            exportSheetsWithEpplusButton.Name = "exportSheetsWithEpplusButton";
            exportSheetsWithEpplusButton.Size = new System.Drawing.Size(126, 50);
            exportSheetsWithEpplusButton.TabIndex = 4;
            exportSheetsWithEpplusButton.Text = "Export Excel";
            exportSheetsWithEpplusButton.UseVisualStyleBackColor = true;
            exportSheetsWithEpplusButton.Click += exportSheetsWithEpplusButton_Click;
            //
            // queryTabPage
            //
            queryTabPage.Controls.Add(sqlEditorRichTextBox);
            queryTabPage.Controls.Add(sqlActionsGroupBox);
            queryTabPage.Controls.Add(queryResultsGridView);
            queryTabPage.Controls.Add(queryActionsGroupBox);
            queryTabPage.Location = new System.Drawing.Point(4, 29);
            queryTabPage.Name = "queryTabPage";
            queryTabPage.Padding = new System.Windows.Forms.Padding(3);
            queryTabPage.Size = new System.Drawing.Size(1032, 570);
            queryTabPage.TabIndex = 2;
            queryTabPage.Text = "DataView";
            queryTabPage.UseVisualStyleBackColor = true;
            //
            // sqlEditorRichTextBox
            //
            sqlEditorRichTextBox.Location = new System.Drawing.Point(42, 158);
            sqlEditorRichTextBox.Name = "sqlEditorRichTextBox";
            sqlEditorRichTextBox.Size = new System.Drawing.Size(983, 136);
            sqlEditorRichTextBox.TabIndex = 2;
            sqlEditorRichTextBox.Text = "";
            //
            // sqlActionsGroupBox
            //
            sqlActionsGroupBox.Controls.Add(generateDeleteSqlButton);
            sqlActionsGroupBox.Controls.Add(generateUpdateSqlButton);
            sqlActionsGroupBox.Controls.Add(executeSqlButton);
            sqlActionsGroupBox.Location = new System.Drawing.Point(821, 3);
            sqlActionsGroupBox.Name = "sqlActionsGroupBox";
            sqlActionsGroupBox.Size = new System.Drawing.Size(204, 153);
            sqlActionsGroupBox.TabIndex = 3;
            sqlActionsGroupBox.TabStop = false;
            sqlActionsGroupBox.Text = "Excute SQL";
            //
            // generateDeleteSqlButton
            //
            generateDeleteSqlButton.Location = new System.Drawing.Point(38, 18);
            generateDeleteSqlButton.Name = "generateDeleteSqlButton";
            generateDeleteSqlButton.Size = new System.Drawing.Size(155, 42);
            generateDeleteSqlButton.TabIndex = 7;
            generateDeleteSqlButton.Text = "Generate Delete";
            generateDeleteSqlButton.UseVisualStyleBackColor = true;
            generateDeleteSqlButton.Click += generateDeleteSqlButton_Click;
            //
            // generateUpdateSqlButton
            //
            generateUpdateSqlButton.Location = new System.Drawing.Point(38, 62);
            generateUpdateSqlButton.Name = "generateUpdateSqlButton";
            generateUpdateSqlButton.Size = new System.Drawing.Size(155, 42);
            generateUpdateSqlButton.TabIndex = 8;
            generateUpdateSqlButton.Text = " Generate Update";
            generateUpdateSqlButton.UseVisualStyleBackColor = true;
            generateUpdateSqlButton.Click += generateUpdateSqlButton_Click;
            //
            // executeSqlButton
            //
            executeSqlButton.Location = new System.Drawing.Point(38, 108);
            executeSqlButton.Name = "executeSqlButton";
            executeSqlButton.Size = new System.Drawing.Size(155, 42);
            executeSqlButton.TabIndex = 9;
            executeSqlButton.Text = "Execute Sql";
            executeSqlButton.UseVisualStyleBackColor = true;
            executeSqlButton.Click += executeSqlButton_Click;
            //
            // queryResultsGridView
            //
            queryResultsGridView.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            queryResultsGridView.Location = new System.Drawing.Point(42, 302);
            queryResultsGridView.Name = "queryResultsGridView";
            queryResultsGridView.RowHeadersWidth = 51;
            queryResultsGridView.Size = new System.Drawing.Size(983, 267);
            queryResultsGridView.TabIndex = 0;
            //
            // queryActionsGroupBox
            //
            queryActionsGroupBox.Controls.Add(exportTableToExcelButton);
            queryActionsGroupBox.Controls.Add(generateSelectSqlButton);
            queryActionsGroupBox.Controls.Add(exportTableToCsvButton);
            queryActionsGroupBox.Controls.Add(previewQueryButton);
            queryActionsGroupBox.Controls.Add(exportQueryToExcelButton);
            queryActionsGroupBox.Controls.Add(queryTableLabel);
            queryActionsGroupBox.Controls.Add(queryTableComboBox);
            queryActionsGroupBox.Location = new System.Drawing.Point(42, 3);
            queryActionsGroupBox.Name = "queryActionsGroupBox";
            queryActionsGroupBox.Size = new System.Drawing.Size(772, 153);
            queryActionsGroupBox.TabIndex = 1;
            queryActionsGroupBox.TabStop = false;
            queryActionsGroupBox.Text = "Data View";
            //
            // exportTableToExcelButton
            //
            exportTableToExcelButton.Location = new System.Drawing.Point(374, 75);
            exportTableToExcelButton.Margin = new System.Windows.Forms.Padding(2);
            exportTableToExcelButton.Name = "exportTableToExcelButton";
            exportTableToExcelButton.Size = new System.Drawing.Size(169, 52);
            exportTableToExcelButton.TabIndex = 7;
            exportTableToExcelButton.Text = "Export Excel By Table";
            exportTableToExcelButton.UseVisualStyleBackColor = true;
            exportTableToExcelButton.Click += exportTableToExcelButton_Click;
            //
            // generateSelectSqlButton
            //
            generateSelectSqlButton.Location = new System.Drawing.Point(374, 27);
            generateSelectSqlButton.Name = "generateSelectSqlButton";
            generateSelectSqlButton.Size = new System.Drawing.Size(169, 42);
            generateSelectSqlButton.TabIndex = 6;
            generateSelectSqlButton.Text = "Generate Select";
            generateSelectSqlButton.UseVisualStyleBackColor = true;
            generateSelectSqlButton.Click += generateSelectSqlButton_Click;
            //
            // exportTableToCsvButton
            //
            exportTableToCsvButton.Location = new System.Drawing.Point(560, 75);
            exportTableToCsvButton.Name = "exportTableToCsvButton";
            exportTableToCsvButton.Size = new System.Drawing.Size(178, 52);
            exportTableToCsvButton.TabIndex = 5;
            exportTableToCsvButton.Text = "Export Csv By Table";
            exportTableToCsvButton.UseVisualStyleBackColor = true;
            exportTableToCsvButton.Click += exportTableToCsvButton_Click;
            //
            // previewQueryButton
            //
            previewQueryButton.Location = new System.Drawing.Point(560, 27);
            previewQueryButton.Name = "previewQueryButton";
            previewQueryButton.Size = new System.Drawing.Size(178, 43);
            previewQueryButton.TabIndex = 3;
            previewQueryButton.Text = "View Data";
            previewQueryButton.UseVisualStyleBackColor = true;
            previewQueryButton.Click += previewQueryButton_Click;
            //
            // exportQueryToExcelButton
            //
            exportQueryToExcelButton.Location = new System.Drawing.Point(156, 77);
            exportQueryToExcelButton.Name = "exportQueryToExcelButton";
            exportQueryToExcelButton.Size = new System.Drawing.Size(178, 48);
            exportQueryToExcelButton.TabIndex = 2;
            exportQueryToExcelButton.Text = "Export Excel By SQL";
            exportQueryToExcelButton.UseVisualStyleBackColor = true;
            exportQueryToExcelButton.Click += exportQueryToExcelButton_Click;
            //
            // queryTableLabel
            //
            queryTableLabel.AutoSize = true;
            queryTableLabel.Location = new System.Drawing.Point(25, 42);
            queryTableLabel.Name = "queryTableLabel";
            queryTableLabel.Size = new System.Drawing.Size(127, 20);
            queryTableLabel.TabIndex = 1;
            queryTableLabel.Text = "Please choose：";
            //
            // queryTableComboBox
            //
            queryTableComboBox.FormattingEnabled = true;
            queryTableComboBox.Location = new System.Drawing.Point(156, 33);
            queryTableComboBox.Name = "queryTableComboBox";
            queryTableComboBox.Size = new System.Drawing.Size(177, 28);
            queryTableComboBox.TabIndex = 0;
            //
            // proceduresTabPage
            //
            proceduresTabPage.Controls.Add(procedureExecutionGroupBox);
            proceduresTabPage.Location = new System.Drawing.Point(4, 29);
            proceduresTabPage.Name = "proceduresTabPage";
            proceduresTabPage.Padding = new System.Windows.Forms.Padding(3);
            proceduresTabPage.Size = new System.Drawing.Size(1032, 570);
            proceduresTabPage.TabIndex = 3;
            proceduresTabPage.Text = "Stored procedure";
            proceduresTabPage.UseVisualStyleBackColor = true;
            //
            // procedureExecutionGroupBox
            //
            procedureExecutionGroupBox.Controls.Add(removeProcedureButton);
            procedureExecutionGroupBox.Controls.Add(addProcedureButton);
            procedureExecutionGroupBox.Controls.Add(selectedProceduresListBox);
            procedureExecutionGroupBox.Controls.Add(executeProceduresButton);
            procedureExecutionGroupBox.Controls.Add(availableProceduresTreeView);
            procedureExecutionGroupBox.Location = new System.Drawing.Point(20, 7);
            procedureExecutionGroupBox.Name = "procedureExecutionGroupBox";
            procedureExecutionGroupBox.Size = new System.Drawing.Size(961, 517);
            procedureExecutionGroupBox.TabIndex = 3;
            procedureExecutionGroupBox.TabStop = false;
            procedureExecutionGroupBox.Text = "Click to choose";
            //
            // removeProcedureButton
            //
            removeProcedureButton.Location = new System.Drawing.Point(403, 156);
            removeProcedureButton.Margin = new System.Windows.Forms.Padding(2);
            removeProcedureButton.Name = "removeProcedureButton";
            removeProcedureButton.Size = new System.Drawing.Size(174, 48);
            removeProcedureButton.TabIndex = 4;
            removeProcedureButton.Text = "Remove a procedure";
            removeProcedureButton.UseVisualStyleBackColor = true;
            removeProcedureButton.Click += removeProcedureButton_Click;
            //
            // addProcedureButton
            //
            addProcedureButton.Location = new System.Drawing.Point(403, 84);
            addProcedureButton.Margin = new System.Windows.Forms.Padding(2);
            addProcedureButton.Name = "addProcedureButton";
            addProcedureButton.Size = new System.Drawing.Size(174, 53);
            addProcedureButton.TabIndex = 3;
            addProcedureButton.Text = "Add a procedure";
            addProcedureButton.UseVisualStyleBackColor = true;
            addProcedureButton.Click += addProcedureButton_Click;
            //
            // selectedProceduresListBox
            //
            selectedProceduresListBox.FormattingEnabled = true;
            selectedProceduresListBox.Location = new System.Drawing.Point(610, 37);
            selectedProceduresListBox.Name = "selectedProceduresListBox";
            selectedProceduresListBox.Size = new System.Drawing.Size(339, 404);
            selectedProceduresListBox.TabIndex = 1;
            selectedProceduresListBox.DoubleClick += selectedProceduresListBox_DoubleClick;
            //
            // executeProceduresButton
            //
            executeProceduresButton.Location = new System.Drawing.Point(598, 458);
            executeProceduresButton.Name = "executeProceduresButton";
            executeProceduresButton.Size = new System.Drawing.Size(142, 40);
            executeProceduresButton.TabIndex = 2;
            executeProceduresButton.Text = "Run";
            executeProceduresButton.UseVisualStyleBackColor = true;
            executeProceduresButton.Click += executeProceduresButton_Click;
            //
            // availableProceduresTreeView
            //
            availableProceduresTreeView.Location = new System.Drawing.Point(43, 37);
            availableProceduresTreeView.Name = "availableProceduresTreeView";
            availableProceduresTreeView.Size = new System.Drawing.Size(344, 401);
            availableProceduresTreeView.TabIndex = 0;
            availableProceduresTreeView.NodeMouseDoubleClick += availableProceduresTreeView_NodeMouseDoubleClick;
            //
            // sqliteMigrationTabPage
            //
            sqliteMigrationTabPage.Controls.Add(refreshSqliteProgressButton);
            sqliteMigrationTabPage.Controls.Add(cancelSqliteExportButton);
            sqliteMigrationTabPage.Controls.Add(sqliteOutputFolderTextBox);
            sqliteMigrationTabPage.Controls.Add(browseSqliteOutputFolderButton);
            sqliteMigrationTabPage.Controls.Add(createSqliteButton);
            sqliteMigrationTabPage.Location = new System.Drawing.Point(4, 29);
            sqliteMigrationTabPage.Name = "sqliteMigrationTabPage";
            sqliteMigrationTabPage.Padding = new System.Windows.Forms.Padding(3);
            sqliteMigrationTabPage.Size = new System.Drawing.Size(1032, 570);
            sqliteMigrationTabPage.TabIndex = 4;
            sqliteMigrationTabPage.Text = "Tools";
            sqliteMigrationTabPage.UseVisualStyleBackColor = true;
            //
            // refreshSqliteProgressButton
            //
            refreshSqliteProgressButton.Location = new System.Drawing.Point(599, 286);
            refreshSqliteProgressButton.Name = "refreshSqliteProgressButton";
            refreshSqliteProgressButton.Size = new System.Drawing.Size(189, 47);
            refreshSqliteProgressButton.TabIndex = 4;
            refreshSqliteProgressButton.Text = "Progress Check";
            refreshSqliteProgressButton.UseVisualStyleBackColor = true;
            refreshSqliteProgressButton.Click += refreshSqliteProgressButton_Click;
            //
            // cancelSqliteExportButton
            //
            cancelSqliteExportButton.Location = new System.Drawing.Point(599, 359);
            cancelSqliteExportButton.Name = "cancelSqliteExportButton";
            cancelSqliteExportButton.Size = new System.Drawing.Size(189, 42);
            cancelSqliteExportButton.TabIndex = 3;
            cancelSqliteExportButton.Text = "Stop";
            cancelSqliteExportButton.UseVisualStyleBackColor = true;
            cancelSqliteExportButton.Click += cancelSqliteExportButton_Click;
            //
            // sqliteOutputFolderTextBox
            //
            sqliteOutputFolderTextBox.Location = new System.Drawing.Point(336, 87);
            sqliteOutputFolderTextBox.Name = "sqliteOutputFolderTextBox";
            sqliteOutputFolderTextBox.ReadOnly = true;
            sqliteOutputFolderTextBox.Size = new System.Drawing.Size(452, 27);
            sqliteOutputFolderTextBox.TabIndex = 1;
            //
            // browseSqliteOutputFolderButton
            //
            browseSqliteOutputFolderButton.Location = new System.Drawing.Point(83, 77);
            browseSqliteOutputFolderButton.Name = "browseSqliteOutputFolderButton";
            browseSqliteOutputFolderButton.Size = new System.Drawing.Size(207, 47);
            browseSqliteOutputFolderButton.TabIndex = 0;
            browseSqliteOutputFolderButton.Text = "Select Output Folder";
            browseSqliteOutputFolderButton.UseVisualStyleBackColor = true;
            browseSqliteOutputFolderButton.Click += browseSqliteOutputFolderButton_Click;
            //
            // createSqliteButton
            //
            createSqliteButton.Location = new System.Drawing.Point(599, 207);
            createSqliteButton.Name = "createSqliteButton";
            createSqliteButton.Size = new System.Drawing.Size(189, 47);
            createSqliteButton.TabIndex = 2;
            createSqliteButton.Text = "Create SQLite";
            createSqliteButton.UseVisualStyleBackColor = true;
            createSqliteButton.Click += createSqliteButton_Click;
            //
            // mainToolStrip
            //
            mainToolStrip.ImageScalingSize = new System.Drawing.Size(20, 20);
            mainToolStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] { databaseConnectionToolStripButton, connectionToolStripSeparator, exitToolStripButton, aboutToolStripSeparator, aboutToolStripButton });
            mainToolStrip.Location = new System.Drawing.Point(0, 0);
            mainToolStrip.Name = "mainToolStrip";
            mainToolStrip.Padding = new System.Windows.Forms.Padding(0, 0, 2, 0);
            mainToolStrip.Size = new System.Drawing.Size(1070, 27);
            mainToolStrip.TabIndex = 2;
            //
            // databaseConnectionToolStripButton
            //
            databaseConnectionToolStripButton.Image = (System.Drawing.Image)resources.GetObject("databaseConnectionToolStripButton.Image");
            databaseConnectionToolStripButton.ImageTransparentColor = System.Drawing.Color.Magenta;
            databaseConnectionToolStripButton.Name = "databaseConnectionToolStripButton";
            databaseConnectionToolStripButton.Size = new System.Drawing.Size(74, 24);
            databaseConnectionToolStripButton.Text = "Login";
            databaseConnectionToolStripButton.ToolTipText = "Login";
            databaseConnectionToolStripButton.Click += databaseConnectionToolStripButton_Click;
            //
            // connectionToolStripSeparator
            //
            connectionToolStripSeparator.Name = "connectionToolStripSeparator";
            connectionToolStripSeparator.Size = new System.Drawing.Size(6, 27);
            //
            // exitToolStripButton
            //
            exitToolStripButton.Image = (System.Drawing.Image)resources.GetObject("exitToolStripButton.Image");
            exitToolStripButton.ImageTransparentColor = System.Drawing.Color.Magenta;
            exitToolStripButton.Name = "exitToolStripButton";
            exitToolStripButton.Size = new System.Drawing.Size(73, 24);
            exitToolStripButton.Text = "Close";
            exitToolStripButton.Click += exitToolStripButton_Click;
            //
            // aboutToolStripSeparator
            //
            aboutToolStripSeparator.Name = "aboutToolStripSeparator";
            aboutToolStripSeparator.Size = new System.Drawing.Size(6, 27);
            //
            // aboutToolStripButton
            //
            aboutToolStripButton.Image = (System.Drawing.Image)resources.GetObject("aboutToolStripButton.Image");
            aboutToolStripButton.ImageTransparentColor = System.Drawing.Color.Magenta;
            aboutToolStripButton.Name = "aboutToolStripButton";
            aboutToolStripButton.Size = new System.Drawing.Size(79, 24);
            aboutToolStripButton.Text = "About";
            aboutToolStripButton.Click += aboutToolStripButton_Click;
            //
            // mainStatusStrip
            //
            mainStatusStrip.ImageScalingSize = new System.Drawing.Size(20, 20);
            mainStatusStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] { operationMessageStatusLabel, operationDetailsStatusLabel });
            mainStatusStrip.Location = new System.Drawing.Point(0, 660);
            mainStatusStrip.Name = "mainStatusStrip";
            mainStatusStrip.Padding = new System.Windows.Forms.Padding(2, 0, 16, 0);
            mainStatusStrip.ShowItemToolTips = true;
            mainStatusStrip.Size = new System.Drawing.Size(1070, 26);
            mainStatusStrip.TabIndex = 3;
            mainStatusStrip.Text = "mainStatusStrip";
            //
            // operationMessageStatusLabel
            //
            operationMessageStatusLabel.Name = "operationMessageStatusLabel";
            operationMessageStatusLabel.Size = new System.Drawing.Size(1052, 20);
            operationMessageStatusLabel.Spring = true;
            operationMessageStatusLabel.Text = "Info:";
            operationMessageStatusLabel.TextAlign = System.Drawing.ContentAlignment.MiddleLeft;
            operationMessageStatusLabel.ToolTipText = "Info";
            //
            // operationDetailsStatusLabel
            //
            operationDetailsStatusLabel.Name = "operationDetailsStatusLabel";
            operationDetailsStatusLabel.Size = new System.Drawing.Size(0, 20);
            operationDetailsStatusLabel.TextAlign = System.Drawing.ContentAlignment.MiddleLeft;
            //
            // Main
            //
            AutoScaleDimensions = new System.Drawing.SizeF(9F, 20F);
            AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            ClientSize = new System.Drawing.Size(1070, 686);
            Controls.Add(mainStatusStrip);
            Controls.Add(mainToolStrip);
            Controls.Add(mainTabControl);
            Icon = (System.Drawing.Icon)resources.GetObject("$this.Icon");
            Name = "Main";
            Text = "DataPie V2026.09（杨福来）";
            FormClosed += Main_FormClosed;
            mainTabControl.ResumeLayout(false);
            importTabPage.ResumeLayout(false);
            importFolderGroupBox.ResumeLayout(false);
            importFolderGroupBox.PerformLayout();
            importFileGroupBox.ResumeLayout(false);
            importFileGroupBox.PerformLayout();
            importTableGroupBox.ResumeLayout(false);
            importTableGroupBox.PerformLayout();
            exportTabPage.ResumeLayout(false);
            exportSelectionGroupBox.ResumeLayout(false);
            exportActionsGroupBox.ResumeLayout(false);
            queryTabPage.ResumeLayout(false);
            sqlActionsGroupBox.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)queryResultsGridView).EndInit();
            queryActionsGroupBox.ResumeLayout(false);
            queryActionsGroupBox.PerformLayout();
            proceduresTabPage.ResumeLayout(false);
            procedureExecutionGroupBox.ResumeLayout(false);
            sqliteMigrationTabPage.ResumeLayout(false);
            sqliteMigrationTabPage.PerformLayout();
            mainToolStrip.ResumeLayout(false);
            mainToolStrip.PerformLayout();
            mainStatusStrip.ResumeLayout(false);
            mainStatusStrip.PerformLayout();
            ResumeLayout(false);
            PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TabControl mainTabControl;
        private System.Windows.Forms.TabPage importTabPage;
        private System.Windows.Forms.TabPage exportTabPage;
        private System.Windows.Forms.ComboBox importTableComboBox;
        private System.Windows.Forms.GroupBox importFileGroupBox;
        private System.Windows.Forms.TextBox importFilePathTextBox;
        private System.Windows.Forms.Button browseImportFileButton;
        private System.Windows.Forms.GroupBox importTableGroupBox;
        private System.Windows.Forms.Label importTableLabel;
        private System.Windows.Forms.Button clearTableDataButton;
        private System.Windows.Forms.Button exportTemplateButton;
        private System.Windows.Forms.ToolStrip mainToolStrip;
        private System.Windows.Forms.ToolStripButton databaseConnectionToolStripButton;
        private System.Windows.Forms.Button importFileButton;
        private System.Windows.Forms.ToolStripButton exitToolStripButton;
        private System.Windows.Forms.TabPage queryTabPage;
        private System.Windows.Forms.GroupBox queryActionsGroupBox;
        private System.Windows.Forms.Button exportQueryToExcelButton;
        private System.Windows.Forms.Label queryTableLabel;
        private System.Windows.Forms.ComboBox queryTableComboBox;
        private System.Windows.Forms.DataGridView queryResultsGridView;
        private System.Windows.Forms.Button previewQueryButton;
        private System.Windows.Forms.RichTextBox sqlEditorRichTextBox;
        private System.Windows.Forms.Button exportTableToCsvButton;
        private System.Windows.Forms.StatusStrip mainStatusStrip;
        private System.Windows.Forms.GroupBox exportActionsGroupBox;
        private System.Windows.Forms.Button exportTablesToCsvButton;
        private System.Windows.Forms.Button exportSheetsWithEpplusButton;
        private System.Windows.Forms.ListBox selectedExportObjectsListBox;
        private System.Windows.Forms.TreeView exportObjectsTreeView;
        private System.Windows.Forms.TabPage proceduresTabPage;
        private System.Windows.Forms.ListBox selectedProceduresListBox;
        private System.Windows.Forms.TreeView availableProceduresTreeView;
        private System.Windows.Forms.Button executeProceduresButton;
        private System.Windows.Forms.GroupBox exportSelectionGroupBox;
        private System.Windows.Forms.GroupBox procedureExecutionGroupBox;
        private System.Windows.Forms.Button clearExportListButton;
        private System.Windows.Forms.Button generateUpdateSqlButton;
        private System.Windows.Forms.Button generateDeleteSqlButton;
        private System.Windows.Forms.Button generateSelectSqlButton;
        private System.Windows.Forms.Button executeSqlButton;
        private System.Windows.Forms.GroupBox sqlActionsGroupBox;
        private System.Windows.Forms.TabPage sqliteMigrationTabPage;
        private System.Windows.Forms.Button browseSqliteOutputFolderButton;
        private System.Windows.Forms.Button createSqliteButton;
        private System.Windows.Forms.TextBox sqliteOutputFolderTextBox;
        private System.Windows.Forms.Button cancelSqliteExportButton;
        private System.Windows.Forms.Button refreshSqliteProgressButton;
        private System.Windows.Forms.ToolStripButton aboutToolStripButton;
        private System.Windows.Forms.ToolStripSeparator connectionToolStripSeparator;
        private System.Windows.Forms.ToolStripSeparator aboutToolStripSeparator;
        private System.Windows.Forms.Button exportTableToExcelButton;
        private System.Windows.Forms.Button exportSheetsWithMiniExcelButton;
        private System.Windows.Forms.GroupBox importFolderGroupBox;
        private System.Windows.Forms.Button importFolderButton;
        private System.Windows.Forms.TextBox importFolderPathTextBox;
        private System.Windows.Forms.Button browseImportFolderButton;
        private System.Windows.Forms.Button removeExportItemButton;
        private System.Windows.Forms.Button addExportItemButton;
        private System.Windows.Forms.Button removeProcedureButton;
        private System.Windows.Forms.Button addProcedureButton;
        private WrappingStatusLabel operationMessageStatusLabel;
        private WrappingStatusLabel operationDetailsStatusLabel;
    }
}
