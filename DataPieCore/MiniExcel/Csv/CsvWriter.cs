﻿using MiniExcelLibs.OpenXml;
using MiniExcelLibs.Utils;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MiniExcelLibs.Csv
{
    internal class CsvWriter : IExcelWriter
    {
        private readonly Stream _stream;
        private readonly CsvConfiguration _configuration;
        private readonly bool _printHeader;
        private readonly object _value;

        public CsvWriter(Stream stream, object value, IConfiguration configuration, bool printHeader)
        {
            this._stream = stream;
            this._configuration = configuration == null ? CsvConfiguration.DefaultConfiguration : (CsvConfiguration)configuration;
            this._printHeader = printHeader;
            this._value = value;
        }

        public void SaveAs()
        {
            var seperator = _configuration.Seperator.ToString();
            var newLine = _configuration.NewLine;

            using (StreamWriter writer = _configuration.StreamWriterFunc(_stream))
            {
                if (_value == null)
                {
                    writer.Write("");
                    return;
                }

                var type = _value.GetType();
                Type genericType = null;

                if (_value is IDataReader)
                {
                    GenerateSheetByIDataReader(_value, seperator, newLine, writer);
                }
                else if (_value is IEnumerable)
                {
                    var values = _value as IEnumerable;
                    List<object> keys = new List<object>();
                    List<ExcelColumnInfo> props = null;
                    string mode = null;

                    // check mode
                    {
                        foreach (var item in values) //TODO: need to optimize
                        {
                            if (item != null && mode == null)
                            {
                                if (item is IDictionary<string, object>)
                                {
                                    var item2 = item as IDictionary<string, object>;
                                    mode = "IDictionary<string, object>";
                                    foreach (var key in item2.Keys)
                                        keys.Add(key);
                                }
                                else if (item is IDictionary)
                                {
                                    var item2 = item as IDictionary;
                                    mode = "IDictionary";
                                    foreach (var key in item2.Keys)
                                        keys.Add(key);
                                }
                                else
                                {
                                    mode = "Properties";
                                    genericType = item.GetType();
                                    props = CustomPropertyHelper.GetSaveAsProperties(genericType,_configuration);
                                }

                                break;
                            }
                        }
                    }

                    //if(mode == null)
                    //    throw new NotImplementedException($"Type {type?.Name} & genericType {genericType?.Name} not Implemented. please issue for me.");

                    if (keys.Count == 0 && props == null)
                    {
                        writer.Write(newLine);
                        return;
                    }

                    if (this._printHeader)
                    {
                        if (props != null)
                        {
                            writer.Write(string.Join(seperator, props.Select(s => CsvHelpers.ConvertToCsvValue(s?.ExcelColumnName))));
                            writer.Write(newLine);
                        }
                        else if (keys.Count > 0)
                        {
                            writer.Write(string.Join(seperator, keys));
                            writer.Write(newLine);
                        }
                        else
                        {
                            throw new InvalidOperationException("Please issue for me.");
                        }
                    }

                    if (mode == "IDictionary<string, object>") //Dapper Row
                        GenerateSheetByDapperRow(writer, _value as IEnumerable, keys.Cast<string>().ToList(), seperator, newLine);
                    else if (mode == "IDictionary") //IDictionary
                        GenerateSheetByIDictionary(writer, _value as IEnumerable, keys, seperator, newLine);
                    else if (mode == "Properties")
                        GenerateSheetByProperties(writer, _value as IEnumerable, props, seperator, newLine);
                    else
                        throw new NotImplementedException($"Type {type?.Name} & genericType {genericType?.Name} not Implemented. please issue for me.");
                }
                else if (_value is DataTable)
                {
                    GenerateSheetByDataTable(writer, _value as DataTable, seperator, newLine);
                }
                else
                {
                    throw new NotImplementedException($"Type {type?.Name} & genericType {genericType?.Name} not Implemented. please issue for me.");
                }
            }
        }

        public async Task SaveAsAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            await Task.Run(() => SaveAs(),cancellationToken).ConfigureAwait(false);
        }

        private void GenerateSheetByIDataReader(object value, string seperator, string newLine, StreamWriter writer)
        {
            var reader = (IDataReader)value;

            int fieldCount = reader.FieldCount;
            if (fieldCount == 0)
                throw new InvalidDataException("fieldCount is 0");

            if (this._printHeader)
            {
                for (int i = 0; i < fieldCount; i++)
                {
                    var columnName = reader.GetName(i);

                    if (i != 0)
                        writer.Write(seperator);
                    writer.Write(CsvHelpers.ConvertToCsvValue(ToCsvString(columnName,null)));
                }
                writer.Write(newLine);
            }

            while (reader.Read())
            {
                for (int i = 0; i < fieldCount; i++)
                {
                    var cellValue = reader.GetValue(i);
                    if (i != 0)
                        writer.Write(seperator);
                    writer.Write(CsvHelpers.ConvertToCsvValue(ToCsvString(cellValue,null)));
                }
                writer.Write(newLine);
            }
        }

        private void GenerateSheetByDataTable(StreamWriter writer, DataTable dt, string seperator, string newLine)
        {
            if (_printHeader)
            {
                writer.Write(string.Join(seperator, dt.Columns.Cast<DataColumn>().Select(s => CsvHelpers.ConvertToCsvValue(s.Caption ?? s.ColumnName))));
                writer.Write(newLine);
            }
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                var first = true;
                for (int j = 0; j < dt.Columns.Count; j++)
                {
                    var cellValue = CsvHelpers.ConvertToCsvValue(ToCsvString(dt.Rows[i][j],null));
                    if (!first)
                        writer.Write(seperator);
                    writer.Write(cellValue);
                    first = false;
                }
                writer.Write(newLine);
            }
        }

        private void GenerateSheetByProperties(StreamWriter writer, IEnumerable value, List<ExcelColumnInfo> props, string seperator, string newLine)
        {
            foreach (var v in value)
            {
                var values = props.Select(s => CsvHelpers.ConvertToCsvValue(ToCsvString(s?.Property.GetValue(v),s)));
                writer.Write(string.Join(seperator, values));
                writer.Write(newLine);
            }
        }

        private void GenerateSheetByIDictionary(StreamWriter writer, IEnumerable value, List<object> keys, string seperator, string newLine)
        {
            foreach (IDictionary v in value)
            {
                var values = keys.Select(key => CsvHelpers.ConvertToCsvValue(ToCsvString(v[key],null)));
                writer.Write(string.Join(seperator, values));
                writer.Write(newLine);
            }
        }

        private void GenerateSheetByDapperRow(StreamWriter writer, IEnumerable value, List<string> keys, string seperator, string newLine)
        {
            foreach (IDictionary<string, object> v in value)
            {
                var values = keys.Select(key => CsvHelpers.ConvertToCsvValue(ToCsvString(v[key],null)));
                writer.Write(string.Join(seperator, values));
                writer.Write(newLine);
            }
        }

        public string ToCsvString(object value, ExcelColumnInfo p)
        {
            if (value == null)
                return "";

            if (value is DateTime dateTime)
            {
                if (p?.ExcelFormat != null)
                {
                    return dateTime.ToString(p.ExcelFormat, _configuration.Culture);
                }
                return _configuration.Culture.Equals(CultureInfo.InvariantCulture) ? dateTime.ToString("yyyy-MM-dd HH:mm:ss", _configuration.Culture) : dateTime.ToString(_configuration.Culture);
            }
            if (p?.ExcelFormat != null && value is IFormattable formattableValue)
            {
                return formattableValue.ToString(p.ExcelFormat, _configuration.Culture);
            }

            return Convert.ToString(value, _configuration.Culture);
        }

    }

}
