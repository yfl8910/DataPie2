using System.Windows.Forms;

namespace DataPieDesktop
{
    internal static class FileDialogs
    {
        public static string ShowSaveDialog(string fileName, string extension)
        {
            using var dialog = new SaveFileDialog
            {
                FileName = fileName,
                DefaultExt = extension,
                Filter = extension switch
                {
                    ".xlsx" => "Excel workbook|*.xlsx",
                    ".csv" => "CSV file|*.csv",
                    ".zip" => "ZIP archive|*.zip",
                    ".db" => "SQLite database|*.db",
                    _ => "All files|*.*"
                }
            };
            return dialog.ShowDialog() == DialogResult.OK ? dialog.FileName : null;
        }
    }
}
