using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace Parquet_Converter
{

    public class Converter: ConvertAdapter
    {
        
    
        ConvertEvent convertEvent;
        

        /// <summary>
        /// входящий путь
        /// </summary>
        string inPath = null;

        /// <summary>
        /// Исходящий путь
        /// </summary>
        string outPath = null;

        /// <summary>
        /// Строка запроса в DAT файле
        /// </summary>
        string queryDatFile = null;

        /// <summary>
        /// файлы
        /// </summary>
        FileInfo[] fileList;

        public Converter(DirectoryPath directoryPath)
        {
            inPath = directoryPath.InPath;
            outPath = directoryPath.OutPath;
            queryDatFile = directoryPath.OueryDatFile;
            convertEvent = new ConvertEvent();
        }

        public void Convert()
        {
            // Получить список файлов в папке, записать в переменную fileList
            Predicate<string> GetFileList = new Predicate<string>((filePath) =>
            {
                var inDir = new DirectoryInfo(filePath);
                if (inDir.Exists)
                {
                    fileList = inDir.GetFiles().ToArray();
                    return true;
                }
                return false;
            });

            // Определить расширение файла, записать в переменную convertEvent.SelectedFileExt
            Predicate<string> GetFileType = new Predicate<string>((filePath) =>
            {
                string fileExtension = Path.GetExtension(filePath);
                switch (fileExtension.ToLower())
                {
                    case ".das":
                        convertEvent.SelectedFileExt = "das";
                        break;
                    case ".dat":
                        convertEvent.SelectedFileExt = "dat";
                        break;
                    default:
                        convertEvent.SelectedFileExt = null;
                        break;
                }
                return convertEvent.SelectedFileExt != null ? true : false;
            });

            // Установить событие, на обработку файла в каталоге
            convertEvent.FileHandlerEvent += FileHandler;

            if (GetFileList(inPath))
            {
                convertEvent.OutFilePath = outPath;
                convertEvent.QueryDatFile = queryDatFile;
                foreach (var filePath in fileList)
                {
                    convertEvent.SelectedFilePath = filePath.FullName;

                    if (GetFileType(convertEvent.SelectedFilePath))
                        convertEvent.OnFileHandlerEvent();
                }

            }

            
        }

    }
}
