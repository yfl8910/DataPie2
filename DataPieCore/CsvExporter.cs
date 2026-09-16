using System;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace DataPieCore
{
    public static class CsvExporter
    {
        private static readonly char[] SpecialCharacters = { ',', '\n', '\r', '"' };

        private static void WriteField(StreamWriter writer, string value)
        {
            if (value.IndexOfAny(SpecialCharacters) < 0)
            {
                writer.Write(value);
                return;
            }
            writer.Write('"');
            writer.Write(value.Replace("\"", "\"\""));
            writer.Write('"');
        }

        private static void WriteRow(IDataReader reader, StreamWriter writer, bool isHeader = false)
        {
            for (int column = 0; column < reader.FieldCount; column++)
            {
                if (column > 0) writer.Write(',');
                WriteField(writer, isHeader ? reader.GetName(column) : reader.GetValue(column)?.ToString() ?? "");
            }
            writer.WriteLine();
        }

        public static int SaveCsv(IDataReader reader, string filePath)
        {
            ArgumentNullException.ThrowIfNull(reader);
            var watch = Stopwatch.StartNew();
            using (var writer = CreateWriter(filePath))
            {
                WriteRow(reader, writer, isHeader: true);
                while (reader.Read()) WriteRow(reader, writer);
            }
            return (int)watch.Elapsed.TotalSeconds;
        }

        public static int SaveCsv(IDataReader reader, string filePath, int rowsPerFile)
        {
            ArgumentNullException.ThrowIfNull(reader);
            if (rowsPerFile <= 0) throw new ArgumentOutOfRangeException(nameof(rowsPerFile));
            var watch = Stopwatch.StartNew();
            int fileNumber = 1;
            int rowCount = 0;
            StreamWriter writer = null;
            try
            {
                writer = CreatePartWriter(filePath, fileNumber);
                WriteRow(reader, writer, isHeader: true);
                while (reader.Read())
                {
                    if (rowCount == rowsPerFile)
                    {
                        writer.Dispose();
                        writer = CreatePartWriter(filePath, ++fileNumber);
                        WriteRow(reader, writer, isHeader: true);
                        rowCount = 0;
                    }
                    WriteRow(reader, writer);
                    rowCount++;
                }
            }
            finally
            {
                writer?.Dispose();
            }
            return (int)watch.Elapsed.TotalSeconds;
        }

        private static StreamWriter CreateWriter(string filePath)
            => new StreamWriter(filePath, false, Encoding.GetEncoding("gb2312"));

        private static StreamWriter CreatePartWriter(string filePath, int fileNumber)
            => CreateWriter(Path.Combine(Path.GetDirectoryName(filePath) ?? "",
                $"{Path.GetFileNameWithoutExtension(filePath)}{fileNumber}.csv"));
    }
}
