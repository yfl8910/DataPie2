using System.Reflection;
using DBUtil;
using DataPieDesktop;

internal static class MainBindingsTests
{
    public static void Run(Main form)
    {
        const BindingFlags instance = BindingFlags.Instance | BindingFlags.NonPublic;
        const BindingFlags helper = BindingFlags.Static | BindingFlags.NonPublic;
        T Field<T>(string name) => (T)typeof(Main).GetField(name, instance).GetValue(form);
        void Invoke(string name, params object[] args) => typeof(Main).GetMethod(name, instance).Invoke(form, args);
        void Check(bool value, string message) { if (!value) throw new Exception(message); }
        var schemaField = typeof(Main).GetField("databaseSchema", instance);
        var schema = new DbSchema
        {
            Name = "Sample",
            Tables = new()
            {
                new() { Name = "ImportTable", Columns = new() { new() { Name = "id", Type = "int" } } },
                new() { Name = "QueryTable", Columns = new() { new() { Name = "id", Type = "int" } } }
            },
            ViewNames = new() { "ViewOne" },
            Procedures = new() { new() { Name = "ProcOne" } }
        };
        schemaField.SetValue(form, schema);
        Invoke("BindDatabaseSchema");
        var import = Field<ComboBox>("comboBox1");
        var query = Field<ComboBox>("comboBox2");
        query.SelectedIndex = 1;
        Check(import.Text == "ImportTable" && query.Text == "QueryTable", "Import and query selections must be independent");
        var databaseType = typeof(Main).Assembly.GetType("DataPieDesktop.AppState").GetField("DatabaseType");
        object originalType = databaseType.GetValue(null);
        try
        {
            databaseType.SetValue(null, "SQLSERVER");
            Invoke("generateSelectSqlButton_Click", form, EventArgs.Empty);
            Check(Field<RichTextBox>("richTextBox1").Text.Contains("QueryTable"), "SQL generation uses the query selection");
        }
        finally { databaseType.SetValue(null, originalType); }

        var tree = Field<TreeView>("treeView1");
        var list = Field<ListBox>("listBox1");
        var add = typeof(Main).GetMethod("AddSelectedNode", helper);
        void Add(TreeNode node) => add.Invoke(null, new object[] { list, node });
        Add(null);
        Add(tree.Nodes[0]);
        Check(list.Items.Count == 0, "Missing selections and group nodes cannot be added");
        Add(tree.Nodes[0].Nodes[0]);
        Add(tree.Nodes[0].Nodes[0]);
        Check(list.Items.Count == 1, "Duplicate selections are ignored");
        Add(tree.Nodes[1].Nodes[0]);
        Check(list.Items.Count == 2, "View nodes remain selectable");
        list.SelectedIndex = -1;
        Invoke("listBox1_DoubleClick", form, EventArgs.Empty);
        Check(list.Items.Count == 2, "Empty selection removal is safe");
        list.SelectedIndex = 0;
        Invoke("removeExportItemButton_Click", form, EventArgs.Empty);
        Check(list.Items.Count == 1, "Remove button uses shared removal");
        var procedures = Field<TreeView>("treeView2");
        procedures.SelectedNode = procedures.Nodes[0].Nodes[0];
        Invoke("addProcedureButton_Click", form, EventArgs.Empty);
        Check(Field<ListBox>("listBox2").Items.Count == 1, "Procedure addition uses shared validation");

        schemaField.SetValue(form, new DbSchema { Name = "Empty" });
        Invoke("BindDatabaseSchema");
        Check(import.SelectedIndex == -1 && query.SelectedIndex == -1, "Empty schemas clear table selections");
        Check(list.Items.Count == 0 && Field<ListBox>("listBox2").Items.Count == 0, "Rebinding clears execution lists");
        Add(tree.Nodes[0]);
        Check(list.Items.Count == 0, "Empty groups cannot be selected as tables");
        using var other = new Main();
        Check(schemaField.GetValue(other) == null, "Database schema belongs to each form instance");
        Console.WriteLine("PASS: independent selections, schema binding, SQL generation and shared list validation.");
    }
}
