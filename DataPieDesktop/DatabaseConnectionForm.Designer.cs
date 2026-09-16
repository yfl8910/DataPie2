
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
            Login = new System.Windows.Forms.Button();
            comboBox1 = new System.Windows.Forms.ComboBox();
            tabControl1 = new System.Windows.Forms.TabControl();
            tabPage3 = new System.Windows.Forms.TabPage();
            groupBox1 = new System.Windows.Forms.GroupBox();
            button7 = new System.Windows.Forms.Button();
            button5 = new System.Windows.Forms.Button();
            textBox4 = new System.Windows.Forms.TextBox();
            button4 = new System.Windows.Forms.Button();
            button6 = new System.Windows.Forms.Button();
            textBox3 = new System.Windows.Forms.TextBox();
            comboBox5 = new System.Windows.Forms.ComboBox();
            comboBox4 = new System.Windows.Forms.ComboBox();
            comboBox3 = new System.Windows.Forms.ComboBox();
            label8 = new System.Windows.Forms.Label();
            label7 = new System.Windows.Forms.Label();
            label6 = new System.Windows.Forms.Label();
            label5 = new System.Windows.Forms.Label();
            label4 = new System.Windows.Forms.Label();
            tabPage4 = new System.Windows.Forms.TabPage();
            button9 = new System.Windows.Forms.Button();
            button8 = new System.Windows.Forms.Button();
            textBox5 = new System.Windows.Forms.TextBox();
            label9 = new System.Windows.Forms.Label();
            tabPage1 = new System.Windows.Forms.TabPage();
            tabPage2 = new System.Windows.Forms.TabPage();
            button10 = new System.Windows.Forms.Button();
            dataGridView1 = new System.Windows.Forms.DataGridView();
            comboBox2 = new System.Windows.Forms.ComboBox();
            label1 = new System.Windows.Forms.Label();
            textBox1 = new System.Windows.Forms.TextBox();
            button3 = new System.Windows.Forms.Button();
            textBox2 = new System.Windows.Forms.TextBox();
            button2 = new System.Windows.Forms.Button();
            label2 = new System.Windows.Forms.Label();
            button1 = new System.Windows.Forms.Button();
            label3 = new System.Windows.Forms.Label();
            tabControl1.SuspendLayout();
            tabPage3.SuspendLayout();
            groupBox1.SuspendLayout();
            tabPage4.SuspendLayout();
            tabPage1.SuspendLayout();
            tabPage2.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)dataGridView1).BeginInit();
            SuspendLayout();
            // 
            // Login
            // 
            Login.Location = new System.Drawing.Point(714, 71);
            Login.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            Login.Name = "Login";
            Login.Size = new System.Drawing.Size(148, 47);
            Login.TabIndex = 0;
            Login.Text = "Login";
            Login.UseVisualStyleBackColor = true;
            Login.Click += Login_Click;
            // 
            // comboBox1
            // 
            comboBox1.FormattingEnabled = true;
            comboBox1.ItemHeight = 24;
            comboBox1.Location = new System.Drawing.Point(183, 74);
            comboBox1.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            comboBox1.Name = "comboBox1";
            comboBox1.Size = new System.Drawing.Size(456, 32);
            comboBox1.TabIndex = 1;
            comboBox1.SelectedIndexChanged += comboBox1_SelectedIndexChanged;
            // 
            // tabControl1
            // 
            tabControl1.Controls.Add(tabPage3);
            tabControl1.Controls.Add(tabPage4);
            tabControl1.Controls.Add(tabPage1);
            tabControl1.Controls.Add(tabPage2);
            tabControl1.Location = new System.Drawing.Point(16, 31);
            tabControl1.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            tabControl1.Name = "tabControl1";
            tabControl1.SelectedIndex = 0;
            tabControl1.Size = new System.Drawing.Size(1141, 602);
            tabControl1.TabIndex = 14;
            // 
            // tabPage3
            // 
            tabPage3.Controls.Add(groupBox1);
            tabPage3.Location = new System.Drawing.Point(4, 33);
            tabPage3.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            tabPage3.Name = "tabPage3";
            tabPage3.Padding = new System.Windows.Forms.Padding(4, 4, 4, 4);
            tabPage3.Size = new System.Drawing.Size(1133, 565);
            tabPage3.TabIndex = 2;
            tabPage3.Text = "SqlServer";
            tabPage3.UseVisualStyleBackColor = true;
            // 
            // groupBox1
            // 
            groupBox1.Controls.Add(button7);
            groupBox1.Controls.Add(button5);
            groupBox1.Controls.Add(textBox4);
            groupBox1.Controls.Add(button4);
            groupBox1.Controls.Add(button6);
            groupBox1.Controls.Add(textBox3);
            groupBox1.Controls.Add(comboBox5);
            groupBox1.Controls.Add(comboBox4);
            groupBox1.Controls.Add(comboBox3);
            groupBox1.Controls.Add(label8);
            groupBox1.Controls.Add(label7);
            groupBox1.Controls.Add(label6);
            groupBox1.Controls.Add(label5);
            groupBox1.Controls.Add(label4);
            groupBox1.Location = new System.Drawing.Point(0, 7);
            groupBox1.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            groupBox1.Name = "groupBox1";
            groupBox1.Padding = new System.Windows.Forms.Padding(4, 4, 4, 4);
            groupBox1.Size = new System.Drawing.Size(1106, 548);
            groupBox1.TabIndex = 2;
            groupBox1.TabStop = false;
            groupBox1.Text = "Sql Server Login Info";
            // 
            // button7
            // 
            button7.Location = new System.Drawing.Point(426, 346);
            button7.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            button7.Name = "button7";
            button7.Size = new System.Drawing.Size(162, 58);
            button7.TabIndex = 4;
            button7.Text = "Login";
            button7.UseVisualStyleBackColor = true;
            button7.Click += button7_Click;
            // 
            // button5
            // 
            button5.Location = new System.Drawing.Point(718, 385);
            button5.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            button5.Name = "button5";
            button5.Size = new System.Drawing.Size(154, 50);
            button5.TabIndex = 1;
            button5.Text = "Server Stop";
            button5.UseVisualStyleBackColor = true;
            button5.Click += button5_Click;
            // 
            // textBox4
            // 
            textBox4.Location = new System.Drawing.Point(246, 218);
            textBox4.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            textBox4.Name = "textBox4";
            textBox4.Size = new System.Drawing.Size(342, 30);
            textBox4.TabIndex = 9;
            // 
            // button4
            // 
            button4.Location = new System.Drawing.Point(718, 310);
            button4.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            button4.Name = "button4";
            button4.Size = new System.Drawing.Size(154, 58);
            button4.TabIndex = 0;
            button4.Text = "Server Start";
            button4.UseVisualStyleBackColor = true;
            button4.Click += button4_Click;
            // 
            // button6
            // 
            button6.Location = new System.Drawing.Point(235, 347);
            button6.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            button6.Name = "button6";
            button6.Size = new System.Drawing.Size(157, 56);
            button6.TabIndex = 3;
            button6.Text = "Test";
            button6.UseVisualStyleBackColor = true;
            button6.Click += button6_Click;
            // 
            // textBox3
            // 
            textBox3.Location = new System.Drawing.Point(246, 164);
            textBox3.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            textBox3.Name = "textBox3";
            textBox3.Size = new System.Drawing.Size(342, 30);
            textBox3.TabIndex = 8;
            // 
            // comboBox5
            // 
            comboBox5.FormattingEnabled = true;
            comboBox5.Location = new System.Drawing.Point(246, 275);
            comboBox5.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            comboBox5.Name = "comboBox5";
            comboBox5.Size = new System.Drawing.Size(341, 32);
            comboBox5.TabIndex = 7;
            // 
            // comboBox4
            // 
            comboBox4.FormattingEnabled = true;
            comboBox4.Location = new System.Drawing.Point(246, 104);
            comboBox4.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            comboBox4.Name = "comboBox4";
            comboBox4.Size = new System.Drawing.Size(341, 32);
            comboBox4.TabIndex = 6;
            comboBox4.Text = "(local)";
            // 
            // comboBox3
            // 
            comboBox3.FormattingEnabled = true;
            comboBox3.Items.AddRange(new object[] { "Windows", "SQL Server" });
            comboBox3.Location = new System.Drawing.Point(246, 48);
            comboBox3.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            comboBox3.Name = "comboBox3";
            comboBox3.Size = new System.Drawing.Size(341, 32);
            comboBox3.TabIndex = 5;
            comboBox3.Text = "Windows";
            comboBox3.SelectedIndexChanged += comboBox3_SelectedIndexChanged;
            // 
            // label8
            // 
            label8.AutoSize = true;
            label8.Location = new System.Drawing.Point(84, 284);
            label8.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            label8.Name = "label8";
            label8.Size = new System.Drawing.Size(83, 24);
            label8.TabIndex = 4;
            label8.Text = "DBname";
            // 
            // label7
            // 
            label7.AutoSize = true;
            label7.Location = new System.Drawing.Point(84, 218);
            label7.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            label7.Name = "label7";
            label7.Size = new System.Drawing.Size(91, 24);
            label7.TabIndex = 3;
            label7.Text = "Password";
            // 
            // label6
            // 
            label6.AutoSize = true;
            label6.Location = new System.Drawing.Point(84, 164);
            label6.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            label6.Name = "label6";
            label6.Size = new System.Drawing.Size(100, 24);
            label6.TabIndex = 2;
            label6.Text = "UserName";
            // 
            // label5
            // 
            label5.AutoSize = true;
            label5.Location = new System.Drawing.Point(84, 104);
            label5.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            label5.Name = "label5";
            label5.Size = new System.Drawing.Size(63, 24);
            label5.TabIndex = 1;
            label5.Text = "Server";
            // 
            // label4
            // 
            label4.AutoSize = true;
            label4.Location = new System.Drawing.Point(84, 52);
            label4.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            label4.Name = "label4";
            label4.Size = new System.Drawing.Size(156, 24);
            label4.TabIndex = 0;
            label4.Text = "Authentication：";
            // 
            // tabPage4
            // 
            tabPage4.Controls.Add(button9);
            tabPage4.Controls.Add(button8);
            tabPage4.Controls.Add(textBox5);
            tabPage4.Controls.Add(label9);
            tabPage4.Location = new System.Drawing.Point(4, 33);
            tabPage4.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            tabPage4.Name = "tabPage4";
            tabPage4.Padding = new System.Windows.Forms.Padding(4, 4, 4, 4);
            tabPage4.Size = new System.Drawing.Size(1133, 565);
            tabPage4.TabIndex = 3;
            tabPage4.Text = "Sqlite";
            tabPage4.UseVisualStyleBackColor = true;
            // 
            // button9
            // 
            button9.Location = new System.Drawing.Point(630, 233);
            button9.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            button9.Name = "button9";
            button9.Size = new System.Drawing.Size(169, 55);
            button9.TabIndex = 3;
            button9.Text = "Login";
            button9.UseVisualStyleBackColor = true;
            button9.Click += button9_Click;
            // 
            // button8
            // 
            button8.Location = new System.Drawing.Point(411, 233);
            button8.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            button8.Name = "button8";
            button8.Size = new System.Drawing.Size(151, 55);
            button8.TabIndex = 2;
            button8.Text = "Browse...";
            button8.UseVisualStyleBackColor = true;
            button8.Click += button8_Click;
            // 
            // textBox5
            // 
            textBox5.Location = new System.Drawing.Point(296, 107);
            textBox5.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            textBox5.Name = "textBox5";
            textBox5.Size = new System.Drawing.Size(573, 30);
            textBox5.TabIndex = 1;
            // 
            // label9
            // 
            label9.AutoSize = true;
            label9.Location = new System.Drawing.Point(161, 115);
            label9.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            label9.Name = "label9";
            label9.Size = new System.Drawing.Size(89, 24);
            label9.TabIndex = 0;
            label9.Text = "File path:";
            // 
            // tabPage1
            // 
            tabPage1.Controls.Add(comboBox1);
            tabPage1.Controls.Add(Login);
            tabPage1.Location = new System.Drawing.Point(4, 33);
            tabPage1.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            tabPage1.Name = "tabPage1";
            tabPage1.Padding = new System.Windows.Forms.Padding(4, 4, 4, 4);
            tabPage1.Size = new System.Drawing.Size(1133, 565);
            tabPage1.TabIndex = 0;
            tabPage1.Text = "All DataBase";
            tabPage1.UseVisualStyleBackColor = true;
            // 
            // tabPage2
            // 
            tabPage2.Controls.Add(button10);
            tabPage2.Controls.Add(dataGridView1);
            tabPage2.Controls.Add(comboBox2);
            tabPage2.Controls.Add(label1);
            tabPage2.Controls.Add(textBox1);
            tabPage2.Controls.Add(button3);
            tabPage2.Controls.Add(textBox2);
            tabPage2.Controls.Add(button2);
            tabPage2.Controls.Add(label2);
            tabPage2.Controls.Add(button1);
            tabPage2.Controls.Add(label3);
            tabPage2.Location = new System.Drawing.Point(4, 33);
            tabPage2.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            tabPage2.Name = "tabPage2";
            tabPage2.Padding = new System.Windows.Forms.Padding(4, 4, 4, 4);
            tabPage2.Size = new System.Drawing.Size(1133, 565);
            tabPage2.TabIndex = 1;
            tabPage2.Text = "Config";
            tabPage2.UseVisualStyleBackColor = true;
            // 
            // button10
            // 
            button10.Location = new System.Drawing.Point(60, 146);
            button10.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            button10.Name = "button10";
            button10.Size = new System.Drawing.Size(129, 35);
            button10.TabIndex = 25;
            button10.Text = "Test";
            button10.UseVisualStyleBackColor = true;
            button10.Click += button10_Click;
            // 
            // dataGridView1
            // 
            dataGridView1.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            dataGridView1.Location = new System.Drawing.Point(60, 197);
            dataGridView1.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            dataGridView1.Name = "dataGridView1";
            dataGridView1.RowHeadersWidth = 51;
            dataGridView1.RowTemplate.Height = 29;
            dataGridView1.Size = new System.Drawing.Size(1046, 341);
            dataGridView1.TabIndex = 15;
            dataGridView1.RowHeaderMouseClick += dataGridView1_RowHeaderMouseClick;
            // 
            // comboBox2
            // 
            comboBox2.FormattingEnabled = true;
            comboBox2.Location = new System.Drawing.Point(701, 41);
            comboBox2.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            comboBox2.Name = "comboBox2";
            comboBox2.Size = new System.Drawing.Size(324, 32);
            comboBox2.TabIndex = 24;
            // 
            // label1
            // 
            label1.AutoSize = true;
            label1.Location = new System.Drawing.Point(60, 49);
            label1.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            label1.Name = "label1";
            label1.Size = new System.Drawing.Size(84, 24);
            label1.TabIndex = 17;
            label1.Text = "Dbname";
            // 
            // textBox1
            // 
            textBox1.Location = new System.Drawing.Point(160, 41);
            textBox1.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            textBox1.Name = "textBox1";
            textBox1.Size = new System.Drawing.Size(400, 30);
            textBox1.TabIndex = 14;
            // 
            // button3
            // 
            button3.Location = new System.Drawing.Point(909, 146);
            button3.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            button3.Name = "button3";
            button3.Size = new System.Drawing.Size(129, 35);
            button3.TabIndex = 22;
            button3.Text = "Delete";
            button3.UseVisualStyleBackColor = true;
            button3.Click += Delete_Click;
            // 
            // textBox2
            // 
            textBox2.Location = new System.Drawing.Point(267, 92);
            textBox2.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            textBox2.Name = "textBox2";
            textBox2.Size = new System.Drawing.Size(758, 30);
            textBox2.TabIndex = 16;
            // 
            // button2
            // 
            button2.Location = new System.Drawing.Point(617, 146);
            button2.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            button2.Name = "button2";
            button2.Size = new System.Drawing.Size(129, 35);
            button2.TabIndex = 21;
            button2.Text = "Update";
            button2.UseVisualStyleBackColor = true;
            button2.Click += Update_Click;
            // 
            // label2
            // 
            label2.AutoSize = true;
            label2.Location = new System.Drawing.Point(601, 49);
            label2.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            label2.Name = "label2";
            label2.Size = new System.Drawing.Size(75, 24);
            label2.TabIndex = 18;
            label2.Text = "Dbtype";
            // 
            // button1
            // 
            button1.Location = new System.Drawing.Point(327, 146);
            button1.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            button1.Name = "button1";
            button1.Size = new System.Drawing.Size(129, 35);
            button1.TabIndex = 20;
            button1.Text = "Add";
            button1.UseVisualStyleBackColor = true;
            button1.Click += Add_Click;
            // 
            // label3
            // 
            label3.AutoSize = true;
            label3.Location = new System.Drawing.Point(60, 98);
            label3.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            label3.Name = "label3";
            label3.Size = new System.Drawing.Size(168, 24);
            label3.TabIndex = 19;
            label3.Text = "ConnectionStrings";
            // 
            // DatabaseConnectionForm
            // 
            AutoScaleDimensions = new System.Drawing.SizeF(11F, 24F);
            AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            ClientSize = new System.Drawing.Size(1207, 655);
            Controls.Add(tabControl1);
            Icon = (System.Drawing.Icon)resources.GetObject("$this.Icon");
            Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            Name = "DatabaseConnectionForm";
            Text = "DataPie V2025.12";
            Load += DatabaseConnectionForm_Load;
            tabControl1.ResumeLayout(false);
            tabPage3.ResumeLayout(false);
            groupBox1.ResumeLayout(false);
            groupBox1.PerformLayout();
            tabPage4.ResumeLayout(false);
            tabPage4.PerformLayout();
            tabPage1.ResumeLayout(false);
            tabPage2.ResumeLayout(false);
            tabPage2.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)dataGridView1).EndInit();
            ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Button Login;
        private System.Windows.Forms.ComboBox comboBox1;
        private System.Windows.Forms.TabControl tabControl1;
        private System.Windows.Forms.TabPage tabPage1;
        private System.Windows.Forms.TabPage tabPage2;
        private System.Windows.Forms.DataGridView dataGridView1;
        private System.Windows.Forms.ComboBox comboBox2;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.TextBox textBox1;
        private System.Windows.Forms.Button button3;
        private System.Windows.Forms.TextBox textBox2;
        private System.Windows.Forms.Button button2;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Button button1;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.TabPage tabPage3;
        private System.Windows.Forms.Button button5;
        private System.Windows.Forms.Button button4;
        private System.Windows.Forms.Button button7;
        private System.Windows.Forms.Button button6;
        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.TextBox textBox4;
        private System.Windows.Forms.TextBox textBox3;
        private System.Windows.Forms.ComboBox comboBox5;
        private System.Windows.Forms.ComboBox comboBox4;
        private System.Windows.Forms.ComboBox comboBox3;
        private System.Windows.Forms.Label label8;
        private System.Windows.Forms.Label label7;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.TabPage tabPage4;
        private System.Windows.Forms.Button button9;
        private System.Windows.Forms.Button button8;
        private System.Windows.Forms.TextBox textBox5;
        private System.Windows.Forms.Label label9;
        private System.Windows.Forms.Button button10;
    }
}

