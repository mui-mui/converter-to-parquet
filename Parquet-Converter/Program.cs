using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace Parquet_Converter
{
    class Program
    {
        static void Main(string[] args)
        {
            Predicate<string> DirCheck = new Predicate<string>((dir) =>
            {
                try
                {
                    if (!(new DirectoryInfo(dir).Exists)) return false;
                    return true;
                }
                catch
                {
                    return false;
                }
            });

            string inPath = null, outPath = null, queryDatFile = null;
            while (true)
            {
                if(inPath == null)
                {
                    Console.WriteLine("=> Введите путь к каталогу с файлами, которые нужно конвертировать:");
                    inPath = Console.ReadLine();
                    if (!DirCheck(inPath))
                    {
                        Console.WriteLine("=> Указанного каталога не существует! Повторите попытку...");
                        inPath = null;
                        continue;
                    }
                    Console.WriteLine("=> Введите через ',' список параметров обрабатываемых DAT-датчиков в формате:\n[ид модуля]:[ид канала]:[digital(true/false)]\n" +
                        "Оставьте поле пустым, если данная опция не нужна или необходимо обработать все датчики в DAT-файлах");
                    queryDatFile = Console.ReadLine();
                }
                if (outPath == null)
                {
                    Console.WriteLine("=> Введите путь к каталогу для сохранения конвертированных файлов:");
                    outPath = Console.ReadLine();
                    if (!DirCheck(outPath))
                    {
                        Console.WriteLine("=> Указанного каталога не существует! Повторите попытку...");
                        outPath = null;
                        continue;
                    }
                }
                break;
            }


            DirectoryPath DirectoryPathObj = new DirectoryPath(inPath, outPath, queryDatFile);
 
            Converter ConverterObj = new Converter(DirectoryPathObj);
            ConverterObj.Convert();

            Console.WriteLine("=> Работа программы завершена!");
            Console.ReadKey();
        }
    }
}
