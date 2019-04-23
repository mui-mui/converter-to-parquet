using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using DASFILELib;
using Parquet;

namespace Parquet_Converter.Engine
{
    internal class DasToParquetEngine
    {
        internal static void Convert(string filePath, string outFilePath)
        {
            Action<double[,], DasFile, string> DasToParqFunc = new Action<double[,], DasFile, string>((_dataDas, _dasFile, _outPath) =>
            {
                List<Parquet.Data.Field> schemaList = new List<Parquet.Data.Field>();
                List<Parquet.Data.DataColumn> dataColumns = new List<Parquet.Data.DataColumn>();

                for (var i = 0; i < _dataDas.GetLength(0); i++)
                {
                    List<double> dataSignals = new List<double>();
                    if (i == 0)
                    {
                        for (var j = 0; j < _dataDas.GetLength(1); j++)
                        {
                            dataSignals.Add(_dasFile.CreationDate.AddSeconds(_dataDas[i, j]).Subtract(new DateTime(1970, 1, 1)).TotalSeconds);
                        }
                    }
                    else
                    {
                        for (var j = 0; j < _dataDas.GetLength(1); j++)
                        {
                            dataSignals.Add(_dataDas[i, j]);
                        }
                    }
                    var column = new Parquet.Data.DataColumn(
                        new Parquet.Data.DataField<double>(_dasFile.Channels[i].Tag.Replace('.', '_')),
                        dataSignals.ToArray());

                    schemaList.Add(column.Field);
                    dataColumns.Add(column);
                }
                var schema = new Parquet.Data.Schema(schemaList);

                using (Stream fstream = File.OpenWrite(Path.Combine($"{_outPath}", $"{_dasFile.FileTitle.Split('.')[0]}.parquet")))
                {
                    using (var parquetWriter = new Parquet.ParquetWriter(schema, fstream))
                    {
                        parquetWriter.CompressionMethod = CompressionMethod.Gzip; // метод сжатия
                        using (Parquet.ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
                        {
                            dataColumns.ForEach(col => groupWriter.WriteColumn(col));
                        }
                    }
                }

            });


            DasFile dasFile = new DasFile();
            try
            {
                dasFile.Open(true, $"{filePath}"); // открытие файла
                double[,] dataDas = dasFile.Read();
                DasToParqFunc(dataDas, dasFile, outFilePath);

                Console.WriteLine($"Файл {Path.GetFileNameWithoutExtension(filePath)}. Успешно конвертирован!");
            }
            catch
            {
                Console.WriteLine($"Файл {Path.GetFileNameWithoutExtension(filePath)}. Ошибка конвертирования!");
            }
            finally
            {
                dasFile.Close();
            }
        }
    }
}
