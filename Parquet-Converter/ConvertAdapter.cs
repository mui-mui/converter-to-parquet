using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using iba.ibaFilesLiteDotNet;
using iba;
using Parquet;
using System.IO;

namespace Parquet_Converter
{
    public class ConvertAdapter : IConvertAdapter
    {

        public void DasToParquet(string filePath, string outFilePath)
        {
            Engine.DasToParquetEngine.Convert(filePath, outFilePath);
        }


        public void DatToParquet(string filePath, string outFilePath, string query)
        {
            //Engine.DatToParquetEngine.Convert(filePath, outFilePath);

            Engine.DatToParquetEngine datToParquetEngine = new Engine.DatToParquetEngine(filePath, outFilePath, query);
            datToParquetEngine.Convert();
        }

        // Обработчик события перебора файлов к каталоге
        public void FileHandler(object source, EventArgs e)
        {
            dynamic s = source;
            string fileExt = s.SelectedFileExt;
            string filePath = s.SelectedFilePath;
            string outFilePath = s.OutFilePath;
            string queryDatFile = s.QueryDatFile;

            switch (fileExt.ToLower())
            {
                case "das":
                    DasToParquet(filePath, outFilePath);
                    break;
                case "dat":
                    DatToParquet(filePath, outFilePath, queryDatFile);
                    break;
                default:
                    throw new NotImplementedException();
            }
        }
    }
}
