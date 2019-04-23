using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Parquet_Converter
{
    interface ISorter
    {
        /// <summary>
        /// Определение расширения файла
        /// </summary>
        /// <returns> Расширение файла </returns>
        string GetFileType();
    }
}
