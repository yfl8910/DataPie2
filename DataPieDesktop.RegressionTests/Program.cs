using System.Reflection;
using DataPieDesktop;
using DataPieCore;

internal static class Program
{
    [STAThread]
    private static void Main()
    {
        ApplicationConfiguration.Initialize();
        using var form = new DataPieDesktop.Main();
        _ = form.Handle;
        SynchronizationContext.SetSynchronizationContext(new WindowsFormsSynchronizationContext());
        Exception failure = null;
        form.BeginInvoke(new Action(async () =>
        {
            try
            {
                ConnectionFormTests.Run();
                MainBindingsTests.Run(form);
                await ConnectionSwitchTests.Run(form);
                await CheckOperations(form);
            }
            catch (Exception ex) { failure = ex; }
            finally { Application.ExitThread(); }
        }));
        Application.Run();
        if (failure != null) throw failure;
        Console.WriteLine("PASS: operation errors, success-only completion, single progress refresh and cleanup.");
    }

    private static async Task CheckOperations(DataPieDesktop.Main form)
    {
        const BindingFlags flags = BindingFlags.Instance | BindingFlags.NonPublic;
        var run = typeof(DataPieDesktop.Main).GetMethod("RunOperationAsync", flags);
        T Field<T>(string name) => (T)typeof(DataPieDesktop.Main).GetField(name, flags).GetValue(form);
        void Check(bool value, string message) { if (!value) throw new Exception(message); }
        Task Run(Action work, Action completed, bool progress = false) =>
            (Task)run.Invoke(form, new object[] { work, completed, "Working", progress });
        Task Refresh() => (Task)typeof(DataPieDesktop.Main).GetMethod("CheckConnectionAsync", flags).Invoke(form, null);

        bool completed = false;
        await Run(() => throw new InvalidOperationException("expected failure"), () => completed = true, true);
        Check(!completed, "Failure must not run success callback");
        Check(Field<ToolStripStatusLabel>("operationMessageStatusLabel").Text.Contains("expected failure"), "Failure is shown");
        Check(!Field<bool>("operationRunning") && !Field<bool>("sqliteExportRunning"), "Failure clears operation state");

        using var release = new ManualResetEventSlim();
        Task active = Run(() => release.Wait(), () => completed = true, true);
        try
        {
            Check(Field<bool>("sqliteExportRunning"), "Export progress starts automatically");
            SqlServerToSQLite.currentProcessTable = "sample";
            SqlServerToSQLite.TotalCopyed = 42;
            Task first = Refresh();
            Task second = Refresh();
            Check(first.IsCompleted && second.IsCompleted, "Repeated progress clicks must not start polling loops");
            Check(Field<ToolStripStatusLabel>("operationDetailsStatusLabel").Text.Contains("42"), "Manual refresh shows current count");
            await Run(() => throw new Exception("Busy operation must not run"), () => throw new Exception("Busy callback must not run"));
            SqlServerToSQLite.TotalCopyed = 84;
            await Task.Delay(5200);
            Check(Field<ToolStripStatusLabel>("operationDetailsStatusLabel").Text.Contains("84"), "Timer refreshes progress without manual clicks");
        }
        finally { release.Set(); await active; }
        Check(completed && !Field<bool>("sqliteExportRunning"), "Successful export stops progress");
        Field<ToolStripStatusLabel>("operationDetailsStatusLabel").Text = "finished";
        await Refresh();
        await Task.Delay(5200);
        Check(Field<ToolStripStatusLabel>("operationDetailsStatusLabel").Text == "finished", "Stopped progress cannot overwrite final status");
        Check(Field<ToolStrip>("mainToolStrip").Enabled, "Inputs are restored");
    }
}
