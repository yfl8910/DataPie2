using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DataPieDesktop
{
    internal static class ImportFiles
    {
        public static List<FileInfo> GetFiles(string directoryPath, bool recursive = false, string extension = "")
        {
            ArgumentException.ThrowIfNullOrEmpty(directoryPath);
            if (!Directory.Exists(directoryPath)) return new List<FileInfo>();
            var searchOption = recursive ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;
            return new DirectoryInfo(directoryPath).EnumerateFiles("*", searchOption)
                .Where(file => string.IsNullOrEmpty(extension)
                    ? file.Extension.Equals(".csv", StringComparison.OrdinalIgnoreCase)
                        || file.Extension.Equals(".xlsx", StringComparison.OrdinalIgnoreCase)
                    : file.Extension.Equals(extension, StringComparison.OrdinalIgnoreCase))
                .ToList();
        }
    }
}
