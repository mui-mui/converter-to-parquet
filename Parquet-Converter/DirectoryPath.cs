using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Parquet_Converter
{
    public class DirectoryPath
    {
        public string InPath { get; set; } = null;
        public string OutPath { get; set; } = null;
        public string OueryDatFile { get; set; } = null;

        public DirectoryPath(string inPath, string outPath, string queryDatFile = null)
        {
            InPath = inPath;
            OutPath = outPath;
            OueryDatFile = queryDatFile;
        }
    }
}
