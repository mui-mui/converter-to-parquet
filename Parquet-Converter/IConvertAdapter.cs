using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Parquet_Converter
{
    /// <summary>
    /// Определение методов конвертирования фалов 
    /// </summary>
    interface IConvertAdapter
    {
        /// <summary>
        /// Конвертирование FDA DAS-файла в parquet-файл
        /// </summary>
        void DasToParquet(string filePath, string outFilePath);

        /// <summary>
        /// Конвертирование IBA DAT-файла в parquet-файл
        /// query - запрос датчиков в dat файлах
        /// </summary>
        void DatToParquet(string filePath, string outFilePath, string query = null);

        /// <summary>
        /// Обработчик файлов в зависимости от расширения
        /// </summary>
        /// <param name="source"></param>
        /// <param name="e"></param>
        void FileHandler(object source, EventArgs e);
    }
}
