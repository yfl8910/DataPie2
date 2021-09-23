using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Configuration;
using DBUtil;
using DataPieCore;
using System.ServiceProcess;



namespace DataPieDesktop
{
    public partial class Form1 : Form
    {
        public static Main main = null;

        int ID = 0;

        string sqlcon;

        string Dbtype;

        IDbAccess dbaccess;

        IList<Dbinfo> _DataBaseList = new List<Dbinfo>();

        DBConfig db = new DBConfig();

        public Form1()
        {
          
            InitializeComponent();
        }

     

        private void Login_Click(object sender, EventArgs e)
        {
            MainfromShow();

            this.Hide();
        }

    
        private async void MainfromShow()
        {
            if (main == null)
            {
                main = new Main();
                main.Show();


            }
            else
            {
                main.Show();
               await main.DataLoad();

            }

            
        }

        private void textBox1_TextChanged(object sender, EventArgs e)
        {

        }

        private void Form1_Load(object sender, EventArgs e)
        {
              //sqlcon = ConfigurationManager.AppSettings["Sqlite"];

              sqlcon = "Data Source=data.db";

              Dbtype = "SQLITE";

              dbaccess = IDBFactory.CreateIDB(sqlcon, Dbtype);


            _DataBaseList = dbaccess.GetDataTable("select * from Dbinfo").ToList<Dbinfo>();

            string[] dbtypes = { "SQLSERVER", "MYSQL", "SQLITE", "POSTGRESQL", "ORACLE" };

            if (_DataBaseList.Count > 0)
            {
                comboBox1.DataSource = _DataBaseList.Select(p => p.Dbname).ToList();
                comboBox1.Enabled = true;
                comboBox1.SelectedIndex = 0;

                dataGridView1.DataSource = _DataBaseList;

            }

            comboBox2.DataSource = dbtypes;


        }

        private void comboBox1_SelectedIndexChanged(object sender, EventArgs e)
        {
            AppState.connStr = _DataBaseList.Where(p => p.Dbname == comboBox1.Text).Select(p => p.ConnectionStrings).FirstOrDefault();
            AppState.Dbtype = _DataBaseList.Where(p=>p.Dbname == comboBox1.Text).Select(p => p.Dbtype).FirstOrDefault();

        }

        private void dataGridView1_RowHeaderMouseClick(object sender, DataGridViewCellMouseEventArgs e)
        {
            ID = Convert.ToInt32(dataGridView1.Rows[e.RowIndex].Cells[0].Value.ToString());

            textBox1.Text = dataGridView1.Rows[e.RowIndex].Cells[1].Value.ToString();
            textBox2.Text = dataGridView1.Rows[e.RowIndex].Cells[2].Value.ToString();
            comboBox2.Text = dataGridView1.Rows[e.RowIndex].Cells[3].Value.ToString();

        }
        //test connection
        private void button10_Click(object sender, EventArgs e)
        {
          var  dbaccesstest = IDBFactory.CreateIDB(textBox2.Text, comboBox2.Text);

            try
            {
                dbaccesstest.conn.Open();
                MessageBox.Show("Test success!");
                dbaccesstest.conn.Close();
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }

            
        }

        private void Add_Click(object sender, EventArgs e)
        {
            if (textBox1.Text != "" && textBox2.Text != "") 
            {

                string sql = string.Format("insert into Dbinfo(Dbname, ConnectionStrings,Dbtype) values('{0}', '{1}', '{2}')", textBox1.Text, textBox2.Text, comboBox2.Text);

                dbaccess.ExecuteSql(sql);

                DisplayData();

                ClearData();

            }
            else
            {
                MessageBox.Show("Please Provide Details!");
            }

        }

        private void Update_Click(object sender, EventArgs e)
        {
            if (textBox1.Text != "" && textBox2.Text != "" && ID>0)
            {

                string sql = string.Format("update Dbinfo set Dbname = '{0}' ,ConnectionStrings = '{1}' ,Dbtype = '{2}' where Id= '{3}'", textBox1.Text, textBox2.Text, comboBox2.Text,ID);

                dbaccess.ExecuteSql(sql);

                DisplayData();

                ClearData();

            }

            else
            {
                MessageBox.Show("Please Select Record to Update");
            }


        }

        private void Delete_Click(object sender, EventArgs e)
        {

            if (ID>0)
            {

                string sql = string.Format("delete from Dbinfo where  Id= {0}", ID);

                dbaccess.ExecuteSql(sql);

                DisplayData();

                ClearData();
            }

            else
            {
                MessageBox.Show("Please Select Record to Delete");
            }
        }

        //Display Data in DataGridView  
        private void DisplayData()
        {
            _DataBaseList = dbaccess.GetDataTable("select * from Dbinfo").ToList<Dbinfo>();

            dataGridView1.DataSource = _DataBaseList;

            comboBox1.DataSource = _DataBaseList.Select(p => p.Dbname).ToList();
            //comboBox1.SelectedIndex = 0;

        }

        //Clear Data  
        private void ClearData()
        {
            textBox1.Text = "";
            textBox2.Text = "";
            ID = 0;
        }

        private void button4_Click(object sender, EventArgs e)
        {

            System.ServiceProcess.ServiceController  sc = new System.ServiceProcess.ServiceController();
            sc.ServiceName = "MSSQLSERVER";
            sc.MachineName = System.Environment.MachineName;

            if (sc == null)
            {
                MessageBox.Show("No SQL SERVER on the machine", "Message");
                return;
            }
            else if (sc.Status != System.ServiceProcess.ServiceControllerStatus.Running)
            {
                sc.Start();
                MessageBox.Show("SQL SERVER started successfully!", "Message");
            }
        }

        private void button5_Click(object sender, EventArgs e)
        {
            System.ServiceProcess.ServiceController sc = new System.ServiceProcess.ServiceController();
            sc.ServiceName = "MSSQLSERVER";
            sc.MachineName = System.Environment.MachineName;
            if (!sc.Status.Equals(System.ServiceProcess.ServiceControllerStatus.Stopped))
            {
                sc.Stop();
                MessageBox.Show(" SQL SERVER Stoped !", "Message");
            }
        }

        private void button6_Click(object sender, EventArgs e)
        {
            if (checkDb() < 0)
            {
                return;
            }
            else
            {
                List<string> _DataBaseList = new List<string>();


                db.ServerName = comboBox4.Text.ToString();

                if (comboBox3.Text == "Windows")
                {
                    db.ValidataType = "Windows";
                }
                else
                {
                    textBox3.Enabled = true;
                    textBox4.Enabled = true;
                    db.ValidataType = "SQLServer";
                    db.UserName = textBox3.Text.ToString();
                    db.UserPwd = textBox4.Text.ToString();

                }

                db.ProviderName = "SQLSERVER";

                sqlcon = db.GetSQLmasterConstring();

                dbaccess = IDBFactory.CreateIDB(sqlcon, db.ProviderName);


                _DataBaseList = dbaccess.GetDataBaseInfo();

                if (_DataBaseList.Count > 0)
                {
                    comboBox5.DataSource = _DataBaseList;
                    comboBox5.Enabled = true;
                    comboBox5.SelectedIndex = 0;
                }

            }

        }

        private void button7_Click(object sender, EventArgs e)
        {
            if (comboBox5.Text.ToString() == "")
            {
                MessageBox.Show("Please select a database!");
                return;
            }

            db.ProviderName = "SQLSERVER";

            db.DataBase = comboBox5.Text.ToString();

            sqlcon = db.GetConstring();


            AppState.connStr = sqlcon;
            AppState.Dbtype = "SQLSERVER";

            var dbaccess1 = IDBFactory.CreateIDB("Data Source=data.db", "SQLITE");
            string sql = string.Format("insert into Dbinfo(Dbname, ConnectionStrings,Dbtype) select '{0}', '{1}', '{2}' WHERE NOT EXISTS(select 1 from Dbinfo where Dbname= '{0}')", comboBox5.Text.ToString(), sqlcon, "SQLSERVER");
            dbaccess1.ExecuteSql(sql);
            
            MainfromShow();

            this.Hide();
        }

        private int checkDb()
        {
            System.ServiceProcess.ServiceController sc = new System.ServiceProcess.ServiceController();
            sc.ServiceName = "MSSQLSERVER";
            if (sc == null)
            {
                MessageBox.Show("No SQL SERVER on the machine", "Message");
                return -1;
            }
            else if (sc.Status != System.ServiceProcess.ServiceControllerStatus.Running)
            {
                MessageBox.Show("Service has not been started. Please click to start SQL service!", "Message");
                return -2;
            }

            return 0;

        }

        private void button8_Click(object sender, EventArgs e)
        {
            OpenFileDialog opeanfile = new OpenFileDialog();
            opeanfile.Filter = ("SQLite|*.db");

            opeanfile.RestoreDirectory = true;
            opeanfile.FilterIndex = 1;
            if (opeanfile.ShowDialog() == DialogResult.OK)
            {
                this.textBox5.Text = opeanfile.FileName;
                textBox5.ReadOnly = true;
            }
        }

        private void button9_Click(object sender, EventArgs e)
        {
            if (textBox5.Text.ToString() == "")
            {
                MessageBox.Show("Please select a database!");
                return;
            }

            DBConfig db = new DBConfig();

            db.ProviderName = "SQLITE";

            db.DataBase = textBox5.Text.ToString();

            sqlcon = db.GetConstring();

            AppState.connStr = sqlcon;
            AppState.Dbtype = "SQLITE";

            MainfromShow();

            this.Hide();
        }

     
    }

    public class Dbinfo
    {

        public int Id { get; set; }
        public string Dbname { get; set; }
        public string ConnectionStrings { get; set; }
        public string Dbtype { get; set; }

    }

    public class DBConfig
    {

        public string ProviderName { get; set; }
        public string ServerName { get; set; }
        public string ValidataType { get; set; }
        public string UserName { get; set; }
        public string UserPwd { get; set; }
        public string DataBase { get; set; }
        public string ConString { get; set; }
     
        public string GetSQLmasterConstring()
        {
            if (ProviderName == "SQLSERVER")
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("Data Source=" + ServerName);
                sb.Append(";Initial Catalog=master ;");
                if (ValidataType == "Windows")
                {
                    sb.Append(" Integrated Security=SSPI;");
                }
                else
                {

                    sb.Append("User ID=" + UserName + ";Password=" + UserPwd + ";");

                }
                return sb.ToString();
            }
            return "";
        }

        public string GetConstring()
        {
            if (ProviderName == "SQLSERVER")
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("Data Source=" + ServerName);
                sb.Append(";Initial Catalog=" + DataBase + " ; ");
                if (ValidataType == "Windows")
                {
                    sb.Append("Integrated Security=SSPI;Connect Timeout=10000");
                }
                else
                {

                    sb.Append("User ID=" + UserName + ";Password=" + UserPwd + ";");

                }
                return sb.ToString();
            }

            else if (ProviderName == "SQLITE")
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("Data Source= " + DataBase + ";");
                return sb.ToString();
            }
            else return "";

        }



    }


}
