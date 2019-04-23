using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Parquet_Converter
{
    /// <summary>
    /// Событие запуска обработки файла в зависимости от его расширения
    /// </summary>
    internal class ConvertEvent
    {
        internal string SelectedFileExt { get; set; } = null;
        internal string SelectedFilePath { get; set; } = null;
        internal string OutFilePath { get; set; } = null;
        internal string QueryDatFile { get; set; } = null;

        internal event EventHandler FileHandlerEvent;

        internal void OnFileHandlerEvent()
        {
            if(FileHandlerEvent != null)
            {
                FileHandlerEvent(this, EventArgs.Empty);
            }
        }
    }
}
