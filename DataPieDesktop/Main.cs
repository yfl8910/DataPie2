using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Text;
using System.Windows.Forms;
using DBUtil;
using DataPieCore;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System.Diagnostics;
using System.IO;




namespace DataPieDesktop
{
    public partial class Main : Form
    {
        public static Form1 loginform = null;

        static IDbAccess dbaccess;

        static DbSchema dbs;

        IList<string> tableList = new List<string>();

        IList<string> viewList = new List<string>();

        IList<string> SpList = new List<string>();

        string tableName = "";

        SynchronizationContext _syncContext = null;

        private Point pi;




        public Main()
        {
            InitializeComponent();
            _syncContext = SynchronizationContext.Current;

        }

        private void toolStrip1_ItemClicked(object sender, ToolStripItemClickedEventArgs e)
        {

        }

        private void tabPage1_Click(object sender, EventArgs e)
        {

        }

        private async void Main_Load(object sender, EventArgs e)
        {
            await DataLoad();
        }

        public async Task DataLoad()
        {

            await Task.Run(() =>
            {
                string ss = "Loading data...";

                this.BeginInvoke(new System.EventHandler(ShowMessage), ss);

                Stopwatch watch = Stopwatch.StartNew();

                watch.Start();

                dbaccess = IDBFactory.CreateIDB(AppState.connStr, AppState.Dbtype);

                dbs = dbaccess.ShowDbSchema();

                tableList = dbs.DbTables.Select(p => p.Name).ToList();

                viewList = dbs.DbViews;

                if (dbs.DbProcs != null)
                {

                    SpList = dbs.DbProcs.Select(p => p.Name).ToList();
                }
                else
                {

                    SpList.Clear();
                }

                SqlWriter.DBtype = AppState.Dbtype;


                _syncContext.Post(SetcomboBox, tableList);//子线程中通过UI线程上下文更新UI 

                watch.Stop();

                ss = string.Format("DataLoad successfully! Time:{0} second.", watch.ElapsedMilliseconds / 1000);

                this.BeginInvoke(new System.EventHandler(ShowMessage), ss);

            });
        }


        private void SetcomboBox(object collection)
        {
            this.comboBox1.DataSource = tableList;
            this.comboBox1.SelectedIndex = 0;

            this.comboBox2.DataSource = tableList;
            this.comboBox2.SelectedIndex = 0;

            statusStrip1.Items[0].Text = dbs.Name;


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

            listBox1.Items.Clear();

            listBox2.Items.Clear();

            textBox1.Text = "";

            textBox2.Text = "";

            richTextBox1.Text = "";


        }



        //Template Export
        private void button1_Click(object sender, EventArgs e)
        {

            string filename = Common.ShowFileDialog(tableName, ".xlsx");

            if (comboBox1.Text.ToString() == "" || filename == null)
            {
                MessageBox.Show("please choose one table and file for save");
            }
            else
            {
                string sql = BuildSQl.GetSQLfromTable(tableName, AppState.Dbtype);

                IDataReader reader = dbaccess.GetDataReader(sql + " where 1=2");

                int i = ExcelIO.SaveMiniExcel(filename, reader, tableName);

                string ss = string.Format("Export success! Time:{0} second", i / 1000);

                this.BeginInvoke(new System.EventHandler(ShowMessage), ss);

            }

        }

        //Delete
        private async void button2_Click(object sender, EventArgs e)
        {

            if (comboBox1.Text.ToString() == "")
            {
                MessageBox.Show("please choose a table!");

                return;
            }


            DialogResult result = MessageBox.Show("Confirm the deletion?", "Message", MessageBoxButtons.OKCancel);

            if (result == DialogResult.OK)
            {

                Stopwatch watch = Stopwatch.StartNew();
                watch.Start();

                this.BeginInvoke(new System.EventHandler(ShowMessage), "deleting…");

                dbaccess.TruncateTable(comboBox1.Text.ToString());

                watch.Stop();

                string ss = string.Format("Delete time:{0} second", watch.ElapsedMilliseconds / 1000);

                this.BeginInvoke(new System.EventHandler(ShowMessage), ss);

            }


        }


        // Import Excel
        private async void button3_Click(object sender, EventArgs e)
        {
            if (textBox1.Text.ToString() == "" || comboBox1.Text.ToString() == "")
            {
                MessageBox.Show("please choose table and file to import!");
            }

            else
            {
                await Task.Run(() =>
                {
                    try
                    {
                        Stopwatch watch = Stopwatch.StartNew();
                        watch.Start();

                        this.BeginInvoke(new System.EventHandler(ShowMessage), "Process…");

                        ImportTheFile(tableName, textBox1.Text.ToString());

                        watch.Stop();

                        string ss = string.Format("Import success, Time:{0} second", watch.ElapsedMilliseconds / 1000);

                        this.BeginInvoke(new System.EventHandler(ShowMessage), ss);

                        GC.Collect();


                    }
                    catch (Exception ex)
                    {
                        throw new Exception(ex.Message);
                    }
                });


            }
        }

        public void ImportTheFile(string DbTableName, string filename)
        {
            try
            {

                string ext = Path.GetExtension(filename).ToLower();

                if (ext == ".xlsx")
                {
                    ExcelIO.MiniExcelReaderImport(filename, DbTableName, dbaccess);
                }

                //else if (ext == ".xls")
                //{
                //    ExcelIO.ExcelDataReaderImport(filename, DbTableName, dbaccess);
                //}

                else if (ext == ".csv")
                {

                    ExcelIO.MiniExcelCsvImport(filename, DbTableName, dbaccess);
                }

                else if (ext == ".db")
                {
                    DbImport(filename, DbTableName, dbaccess);

                }

                else
                {
                    throw new Exception("Not support!");
                }


            }
            catch (System.Exception ex)
            {
                throw ex;
            }

        }

        public void DbImport(string filePath, string tableName, IDbAccess dbAccess)
        {
            DBConfig db = new DBConfig();

            db.ProviderName = "SQLITE";

            db.DataBase = filePath;

            string sqlcon = db.GetConstring();

            var dbaccess = IDBFactory.CreateIDB(sqlcon, "SQLITE");

            var reader = dbaccess.GetDataReader(BuildSQl.GetSQLfromTable(tableName, "SQLITE"));

            try
            {
                dbAccess.BulkInsert(tableName, reader);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                reader.Close();
            }
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

            loginform = new Form1();
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
                await GetViewData(richTextBox1.Text);
            }
            else
            {
                string sql = SqlWriter.WriteSelect(dbs.DbTables.Where(p => p.Name == tableName).FirstOrDefault(), 1000);

                richTextBox1.Text = sql;

                await GetViewData(sql);
            }

        }

        public async Task GetViewData(string Sql)
        {
            await Task.Run(() =>
            {

                try
                {
                    Stopwatch watch = Stopwatch.StartNew();

                    watch.Start();

                    this.BeginInvoke(new System.EventHandler(ShowMessage), "Process…");

                    DataTable dt;

                    dt = dbaccess.GetDataTable(Sql);

                    watch.Stop();

                    string ss = string.Format("Execute successful, Time:{0} second ,Rows: " + dt.Rows.Count, watch.ElapsedMilliseconds / 1000);

                    this.BeginInvoke(new System.EventHandler(ShowMessage), ss);

                    _syncContext.Post(SetDatagrid, dt);//子线程中通过UI线程上下文更新UI 

                }
                catch (System.Exception ex)
                {
                    this.BeginInvoke(new System.EventHandler(ShowErr), ex);

                }

            });

        }

        private void SetDatagrid(object collection)
        {
            this.dataGridView1.DataSource = collection;

        }


        //OUTPUT CSV
        private async void button7_Click(object sender, EventArgs e)
        {
            try
            {
                string filename = Common.ShowFileDialog(tableName, ".csv");
                if (filename != null)
                {
                    await WriteCsvFromsql(tableName, filename);

                }

            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }

        public async Task WriteCsvFromsql(string tableName, string filename)
        {
            await Task.Run(() =>
            {

                try
                {
                    this.BeginInvoke(new System.EventHandler(ShowMessage), "Process…");

                    string sql = BuildSQl.GetSQLfromTable(tableName, AppState.Dbtype);

                    IDataReader reader = dbaccess.GetDataReader(sql);

                    var t = DBToCsv.SaveCsv(reader, filename);

                    string s = string.Format("Export successful! Time :{0} seconds", t);

                    this.BeginInvoke(new System.EventHandler(ShowMessage), s);

                    GC.Collect();
                }
                catch (System.Exception ex)
                {
                    this.BeginInvoke(new System.EventHandler(ShowErr), ex);
                }

            });

        }



        //Export Excel by sql
        private async void button4_Click(object sender, EventArgs e)
        {
            if (richTextBox1.Text.Length == 0)
            {
                MessageBox.Show("Empty SQL for Export");
            }
            else
            {
                string filename = Common.ShowFileDialog(tableName, ".xlsx");

                if (filename != null)
                {
                    await WriteExcelFromsql(richTextBox1.Text.ToString(), filename);

                }

            }
        }


        //Export Excel by tableName
        private async void button16_Click(object sender, EventArgs e)
        {
            if (tableName == "")
            {
                MessageBox.Show("Please choose a table");
            }
            else
            {
                string filename = Common.ShowFileDialog(tableName, ".xlsx");

                if (filename != null)
                {
                    string sql = BuildSQl.GetSQLfromTable(tableName, AppState.Dbtype);

                    await WriteExcelFromsql(sql, filename);

                }

            }
        }

        public async Task WriteExcelFromsql(string sql, string filename)
        {
            await Task.Run(() =>
            {

                try
                {
                    this.BeginInvoke(new System.EventHandler(ShowMessage), "Processing…");

                    //string sql = BuildSQl.GetSQLfromTable(tableName, AppState.Dbtype);

                    IDataReader reader = dbaccess.GetDataReader(sql);

                    //int i = ExcelIO.SaveExcel(filename, reader, tableName);

                    int i = ExcelIO.SaveMiniExcel(filename, reader, tableName);

                    string s = string.Format("Export successful! Time :{0} seconds", i);

                    this.BeginInvoke(new System.EventHandler(ShowMessage), s);

                    GC.Collect();
                }
                catch (System.Exception ex)
                {
                    this.BeginInvoke(new System.EventHandler(ShowErr), ex);
                }

            });

        }


        private void ShowMessage(object o, System.EventArgs e)
        {
            statusStrip1.Items[0].Text = AppState.DbName + "-" + o.ToString();
            statusStrip1.Items[0].ForeColor = Color.Red;
        }

        private void ShowErr(object o, System.EventArgs e)
        {
            Exception ee = o as Exception;

            statusStrip1.Items[0].Text = AppState.DbName + "-" + "Error! " + ee.Message;
            statusStrip1.Items[0].ForeColor = Color.Red;
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
        private async void button10_Click(object sender, EventArgs e)
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

            string filename = Common.ShowFileDialog(SheetNames[0], ".xlsx");

            if (filename != null)
            {
                await WriteMutiExcelFromsql(SheetNames, filename);

            }
        }

        public async Task WriteMutiExcelFromsql(IList<string> TableNames, string filename)
        {
            await Task.Run(() =>
            {

                try
                {
                    this.BeginInvoke(new System.EventHandler(ShowMessage), "Processing…");

                    //int i = ExcelIO.SaveMutiExcel(TableNames, filename, dbaccess, AppState.Dbtype);
                    int i = ExcelIO.SaveMutiMiniExcel(TableNames, filename, dbaccess, AppState.Dbtype);
                
                    string s = string.Format("Export successful! Time :{0} seconds", i);

                    this.BeginInvoke(new System.EventHandler(ShowMessage), s);

                    GC.Collect();
                }

                catch (System.Exception ex)
                {
                    this.BeginInvoke(new System.EventHandler(ShowErr), ex);
                }

            });

        }


        public async Task WriteMutiMiniExcelFromsql(IList<string> TableNames, string filename)
        {
            await Task.Run(() =>
            {

                try
                {
                    this.BeginInvoke(new System.EventHandler(ShowMessage), "Processing…");

                    int i = ExcelIO.SaveMutiMiniExcel(TableNames, filename, dbaccess, AppState.Dbtype);

                    string s = string.Format("Export successful! Time :{0} seconds", i);

                    this.BeginInvoke(new System.EventHandler(ShowMessage), s);

                    GC.Collect();
                }

                catch (System.Exception ex)
                {
                    this.BeginInvoke(new System.EventHandler(ShowErr), ex);
                }

            });

        }


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

            string filename = Common.ShowFileDialog(SheetNames[0], ".csv");

            if (filename != null)
            {
                await WriteMutiCsvFromsql(SheetNames, filename, AppState.Dbtype);

            }

        }

        public async Task WriteMutiCsvFromsql(IList<string> TableNames, string filename, string dbtype)
        {
            await Task.Run(() =>
            {

                try
                {
                    this.BeginInvoke(new System.EventHandler(ShowMessage), "Process…");
                    int time = 0;
                    int count = TableNames.Count();
                    string ss = filename.Substring(0, filename.LastIndexOf("\\"));

                    for (int i = 0; i < count; i++)
                    {
                        StringBuilder newfileName = new StringBuilder(ss);
                        newfileName.Append("\\" + TableNames[i] + ".csv");
                        FileInfo newFile = new FileInfo(newfileName.ToString());
                        if (newFile.Exists)
                        {
                            newFile.Delete();
                            newFile = new FileInfo(newfileName.ToString());
                        }

                        //string sql = "select * from " + TableNames[i] ;
                        string sql = BuildSQl.GetSQLfromTable(TableNames[i], dbtype);

                        IDataReader reader = dbaccess.GetDataReader(sql);

                        int t = DBToCsv.SaveCsv(reader, newfileName.ToString());

                        time += t;


                        reader.Close();

                    }

                    string s = string.Format("Export successful! Time :{0} seconds", time);

                    this.BeginInvoke(new System.EventHandler(ShowMessage), s);

                    GC.Collect();
                }

                catch (System.Exception ex)
                {
                    this.BeginInvoke(new System.EventHandler(ShowErr), ex);
                }

            });

        }


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

                await ProcExeute(list);
            }
        }

        public async Task ProcExeute(IList<string> procs)
        {

            await Task.Run(() =>
            {
                Stopwatch watch = Stopwatch.StartNew();
                watch.Start();

                this.BeginInvoke(new System.EventHandler(ShowMessage), "Processing…");


                try
                {
                    foreach (var item in procs)
                    {
                        int i = dbaccess.RunProcedure(item.ToString());

                    }
                }
                catch (Exception ee)
                {
                    this.BeginInvoke(new System.EventHandler(ShowErr), ee);
                    return;
                }

                watch.Stop();

                string ss = string.Format("Procedure Execute successfully! Time:{0} second.", watch.ElapsedMilliseconds / 1000);
                this.BeginInvoke(new System.EventHandler(ShowMessage), ss);
                return;
            });
        }

        private async void button9_Click(object sender, EventArgs e)
        {
            string sql = SqlWriter.WriteSelect(dbs.DbTables.Where(p => p.Name == tableName).FirstOrDefault(), 1000);
            richTextBox1.Text = sql;
            this.BeginInvoke(new System.EventHandler(ShowMessage), "Select Sql Generated");

        }

        private async void button13_Click(object sender, EventArgs e)
        {
            string sql = SqlWriter.WriteDelete(dbs.DbTables.Where(p => p.Name == tableName).FirstOrDefault());
            richTextBox1.Text = sql;
            this.BeginInvoke(new System.EventHandler(ShowMessage), "Delete Sql Generated");

        }

        private async void button14_Click(object sender, EventArgs e)
        {
            string sql = SqlWriter.WriteUpdate(dbs.DbTables.Where(p => p.Name == tableName).FirstOrDefault());
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
            await Task.Run(() =>
            {

                try
                {
                    Stopwatch watch = Stopwatch.StartNew();

                    watch.Start();

                    this.BeginInvoke(new System.EventHandler(ShowMessage), "Process…");


                    int i = dbaccess.ExecuteSql(Sql);

                    watch.Stop();

                    string ss = string.Format("Execute success, Time:{0} second,Affect " + i + " Rows", watch.ElapsedMilliseconds / 1000);

                    this.BeginInvoke(new System.EventHandler(ShowMessage), ss);

                }
                catch (System.Exception ex)
                {
                    this.BeginInvoke(new System.EventHandler(ShowErr), ex);
                }

            });

        }

        private async void button15_Click(object sender, EventArgs e)
        {

            string filename = Common.ShowFileDialog(dbs.Name + ".db", ".db");

            if (filename != null && AppState.Dbtype == "SQLSERVER")
            {
                textBox2.Text = filename;

                await CreateSqlite(textBox2.Text, null);
            }

            else
            {
                statusStrip1.Items[1].Text = "Only support SQL server To Sqlite";
                statusStrip1.Items[1].ForeColor = Color.Red;
            }
        }

        public async Task CreateSqlite(string filename, string password)
        {
            await Task.Run(() =>
            {

                try
                {
                    Stopwatch watch = Stopwatch.StartNew();

                    watch.Start();

                    this.BeginInvoke(new System.EventHandler(ShowMessage), " Processing…");

                    SqlServerToSQLite.dbs = dbs;

                    SqlServerToSQLite.CreateSQLiteDatabase(filename, null, false);

                    SqlServerToSQLite.CopySqlServerRowsToSQLiteDB(dbaccess.ConnectionString, filename, password);


                    watch.Stop();

                    string ss = string.Format("Sqlite careate successful! Time: {0} seconds, Copy Rows: {1} ", watch.ElapsedMilliseconds / 1000, SqlServerToSQLite.TotalCopyed);

                    this.BeginInvoke(new System.EventHandler(ShowMessage), ss);

                }
                catch (System.Exception ex)
                {
                    this.BeginInvoke(new System.EventHandler(ShowErr), ex);
                }

            });


        }

        public async Task checkStatus()
        {

            await Task.Run(() =>
            {

                try
                {

                    while (!SqlServerToSQLite.Done)

                    {
                        string ss = string.Format("Current Table: {0}, Copy Rows: {1}", SqlServerToSQLite.currentProcessTable, SqlServerToSQLite.TotalCopyed);
                        this.BeginInvoke(new System.EventHandler(ShowMessage), ss);
                        Thread.Sleep(1000);
                    }

                }
                catch (System.Exception ex)
                {
                    this.BeginInvoke(new System.EventHandler(ShowErr), ex);
                }

            });

        }


        private void button17_Click(object sender, EventArgs e)
        {
            SqlServerToSQLite._cancelled = true;

            statusStrip1.Items[1].Text = "Stoped, Copyed Rows:" + SqlServerToSQLite.TotalCopyed;
            statusStrip1.Items[1].ForeColor = Color.Red;
        }

        private async void button18_Click(object sender, EventArgs e)
        {
            await checkStatus();

        }

        private void toolStripButton3_Click(object sender, EventArgs e)
        {
            AboutDataPie about = new AboutDataPie();
            about.Show();
        }

        private async void button19_Click(object sender, EventArgs e)
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

            string filename = Common.ShowFileDialog(SheetNames[0], ".xlsx");

            if (filename != null)
            {
                await WriteMutiMiniExcelFromsql(SheetNames, filename);

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
            if (textBox3.Text.ToString() == "" || comboBox1.Text.ToString() == "")
            {
                MessageBox.Show("please choose table and fold to import!");
            }

            else
            {
                try
                {
                    await ImportTheFolder(tableName, textBox3.Text.ToString());
                }
                catch (Exception ex)
                {
                    throw new Exception(ex.Message);
                }
            }
        }

        public async Task ImportTheFolder(string DbTableName, string path)
        {
            await Task.Run(() =>
            {

                try
                {
                    Stopwatch watch = Stopwatch.StartNew();
                    watch.Start();

                    this.BeginInvoke(new System.EventHandler(ShowMessage), "Process…");

                    List<FileInfo> filelist = Common.FileList(path, false, "");

                    for (int i = 0; i < filelist.Count(); i++)
                    {
                        try
                        {
                            string m = "uploading " + (i + 1) + " file:" + filelist[i].ToString();

                            this.BeginInvoke(new System.EventHandler(ShowMessage), m);

                            ImportTheFile(DbTableName, filelist[i].ToString());

                        }
                        catch (Exception ee)
                        {
                            this.BeginInvoke(new System.EventHandler(ShowErr), ee);
                            return;
                        }
                    }

                    watch.Stop();

                    string ss = string.Format("Import success, Time:{0} second", watch.ElapsedMilliseconds / 1000);

                    this.BeginInvoke(new System.EventHandler(ShowMessage), ss);

                    GC.Collect();


                }
                catch (System.Exception ex)
                {
                    this.BeginInvoke(new System.EventHandler(ShowErr), ex);

                    throw ex;
                }

            });

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
