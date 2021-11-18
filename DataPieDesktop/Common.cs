using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;


namespace DataPieDesktop
{
    public class Common
    {

        #region 弹出FileDialog对话框

        public static string ShowFileDialog(string FileName, string ext)
        {
            System.Windows.Forms.SaveFileDialog saveFileDialog1 = new System.Windows.Forms.SaveFileDialog();

            switch (ext)
            {
                case ".xlsx":
                    saveFileDialog1.Filter = "EXCEL 2007|*.xlsx";
                    saveFileDialog1.FileName = FileName;
                    saveFileDialog1.DefaultExt = ".xlsx";
                    break;
                case ".accdb":
                    saveFileDialog1.Filter = "ACCESS数据库|*.accdb";
                    saveFileDialog1.FileName = FileName;
                    saveFileDialog1.DefaultExt = ".accdb";
                    break;
                case ".csv":
                    saveFileDialog1.Filter = "CSV文件|*.csv";
                    saveFileDialog1.FileName = FileName;
                    saveFileDialog1.DefaultExt = ".csv";
                    break;
                case ".zip":
                    saveFileDialog1.Filter = "zip压缩文件|*.zip";
                    saveFileDialog1.FileName = FileName;
                    saveFileDialog1.DefaultExt = ".zip";
                    break;
                default:
                    break;

            }

            saveFileDialog1.FileName = FileName;

            if (saveFileDialog1.ShowDialog() == System.Windows.Forms.DialogResult.OK)
            {
                return saveFileDialog1.FileName.ToString();
            }
            else
            {
                return null;
            }
        }

        #endregion

        public static List<FileInfo> FileList(string DirectoryPath, bool Recursive = false, string FileTypeExtension = "")
        {
            if (string.IsNullOrEmpty(DirectoryPath))
                throw new ArgumentNullException("DirectoryPath");
            List<FileInfo> Files = new List<FileInfo>();
            if (DirectoryExists(DirectoryPath))
            {
                DirectoryInfo Directory = new DirectoryInfo(DirectoryPath);
                Files.AddRange(Directory.GetFiles());
                if (Recursive)
                {
                    DirectoryInfo[] SubDirectories = Directory.GetDirectories();
                    foreach (DirectoryInfo SubDirectory in SubDirectories)
                    {
                        Files.AddRange(FileList(SubDirectory.FullName, true, FileTypeExtension));
                    }
                }
            }
            if (!string.IsNullOrEmpty(FileTypeExtension))
                return Files.FindAll(x => x.Extension == FileTypeExtension);
            return Files;
        }

        public static bool DirectoryExists(string DirectoryPath)
        {
            if (string.IsNullOrEmpty(DirectoryPath))
                throw new ArgumentNullException("DirectoryPath");
            return Directory.Exists(DirectoryPath);
        }



    }
}
