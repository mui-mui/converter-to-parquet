using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using IBA = iba.ibaFilesLiteDotNet;
using System.IO;
using System.Collections;
using Parq = Parquet.Data;

namespace Parquet_Converter.Engine
{
    struct DatFileStruct
    {
        public int ModuleId { get; set; }
        public int Channelid { get; set; }
        public string ChannelName { get; set; }
        public string ModuleName { get; set; }
        public double StartDate { get; set; }
        public string DataChannel { get; set; }
        public bool Digital { get; set; }
        public string Unit { get; set; }
        public float Offset { get; set; }
    }

    struct QueryDatFileStruct
    {
        public int ModuleId { get; set; }
        public int Channelid { get; set; }
        public bool Digital { get; set; }

        public QueryDatFileStruct(string query)
        {
            string[] queryArgs = query.Split(':');
            ModuleId = Convert.ToInt32(queryArgs[0]);
            Channelid = Convert.ToInt32(queryArgs[1]);
            Digital = Convert.ToBoolean(queryArgs[2]);
        }
    }
        internal class DatToParquetEngine
    {
        // Перевод DateTime в Unix timestamp
        Func<DateTime, double> DateTimeToUnixTimestampFunc = (dateTime) =>
         {
             DateTime unixStart = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
             long unixTimeStampInTicks = (dateTime.ToUniversalTime() - unixStart).Ticks;
             return (double)unixTimeStampInTicks / TimeSpan.TicksPerSecond;
         };

        IBA.IbaFileReader fileReader;
        string filePath, outFilePath;
        List<QueryDatFileStruct> queryDatFileStructsCollection = new List<QueryDatFileStruct>();

        public DatToParquetEngine(string filePath, string outFilePath, string query = null)
        {
            this.filePath = filePath;
            this.outFilePath = outFilePath;

            fileReader = new IBA.IbaFileReader();
            fileReader.Open(filePath);

            if(query != null & query.Length > 0)
            {
                foreach(var q in query.Split(','))
                {
                    queryDatFileStructsCollection.Add(new QueryDatFileStruct(q.Trim()));
                }
            }
        }

        /// <summary>
        /// Конвертер в parquet
        /// </summary>
        public void Convert()
        {
            try
            {
                List<Parq.Field> schemaList = new List<Parq.Field>();
                List<Parq.DataColumn> dataColumns = new List<Parq.DataColumn>();

                DatFileStruct[] datFileStructsCollection = queryDatFileStructsCollection.Count > 0 ? GetDatFileStructCollection(queryDatFileStructsCollection).ToArray() : GetDatFileStructCollection().ToArray();

               dataColumns.Add(new Parq.DataColumn(new Parq.DataField<int>("ModuleId"), datFileStructsCollection.Select(st => st.ModuleId).ToArray()));
                dataColumns.Add(new Parq.DataColumn(new Parq.DataField<int>("Channelid"), datFileStructsCollection.Select(st => st.Channelid).ToArray()));
                dataColumns.Add(new Parq.DataColumn(new Parq.DataField<string>("ChannelName"), datFileStructsCollection.Select(st => st.ChannelName).ToArray()));
                dataColumns.Add(new Parq.DataColumn(new Parq.DataField<string>("ModuleName"), datFileStructsCollection.Select(st => st.ModuleName).ToArray()));
                dataColumns.Add(new Parq.DataColumn(new Parq.DataField<double>("StartDate"), datFileStructsCollection.Select(st => st.StartDate).ToArray()));
                dataColumns.Add(new Parq.DataColumn(new Parq.DataField<string>("DataChannel"), datFileStructsCollection.Select(st => st.DataChannel).ToArray()));
                dataColumns.Add(new Parq.DataColumn(new Parq.DataField<bool>("Digital"), datFileStructsCollection.Select(st => st.Digital).ToArray()));
                dataColumns.Add(new Parq.DataColumn(new Parq.DataField<string>("Unit"), datFileStructsCollection.Select(st => st.Unit).ToArray()));
                dataColumns.Add(new Parq.DataColumn(new Parq.DataField<float>("Offset"), datFileStructsCollection.Select(st => st.Offset).ToArray()));

                schemaList.Add(new Parq.DataField<int>("ModuleId"));
                schemaList.Add(new Parq.DataField<int>("Channelid"));
                schemaList.Add(new Parq.DataField<string>("ChannelName"));
                schemaList.Add(new Parq.DataField<string>("ModuleName"));
                schemaList.Add(new Parq.DataField<double>("StartDate"));
                schemaList.Add(new Parq.DataField<string>("DataChannel"));
                schemaList.Add(new Parq.DataField<bool>("Digital"));
                schemaList.Add(new Parq.DataField<string>("Unit"));
                schemaList.Add(new Parq.DataField<float>("Offset"));

                var schema = new Parq.Schema(schemaList);

                using (Stream fstream = File.OpenWrite(Path.Combine(outFilePath, Path.GetFileNameWithoutExtension(filePath) + ".parquet")))
                {
                    using (var parquetWriter = new Parquet.ParquetWriter(schema, fstream))
                    {
                        parquetWriter.CompressionMethod = Parquet.CompressionMethod.Gzip; // метод сжатия
                        using (Parquet.ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
                        {
                            dataColumns.ForEach(col => groupWriter.WriteColumn(col));
                        }
                    }
                }
                Console.WriteLine($"Файл {Path.GetFileNameWithoutExtension(filePath)}. Успешно конвертирован!");
            }
            catch
            {
                Console.WriteLine($"Файл {Path.GetFileNameWithoutExtension(filePath)}. Ошибка конвертирования!");
            }
        }

        /// <summary>
        /// Получить коллекцию структур DatFileStruct
        /// </summary>
        /// <returns> List<DatFileStruct> </returns>
        List<DatFileStruct> GetDatFileStructCollection()
        {
            // для вывода результатов запроса
            float xBase = 0;
            float xOffset = 0;
            object dataChannel;

            List<DatFileStruct> datFileStructsCollection = new List<DatFileStruct>();

            var modules = fileReader.Modules; // получить коллекцию модулей в файле

            // Заполнение коллекции структур
            foreach(var module in modules)
            {
                // Заполенение структуры
                DatFileStruct datFileStruct = new DatFileStruct();

                datFileStruct.ModuleId = module.Key;
                datFileStruct.ModuleName = module.Value.Name;
                datFileStruct.StartDate = DateTimeToUnixTimestampFunc(fileReader.StartTime); // формат unix timestamp
                foreach (var channel in module.Value.Channels)
                {
                    try
                    {
                        channel.QueryData(out xBase, out xOffset, out dataChannel); // выполнение запроса к каналу
                        IBA.ChannelID channelID = channel.GetID(); // для получения ИД канала
                        datFileStruct.Channelid = channelID.NumberInModule;
                        datFileStruct.Digital = channel.Digital;
                        datFileStruct.Unit = channel.Unit;
                        datFileStruct.ChannelName = channel.Name;
                        
                        dynamic dataChannelDyn = dataChannel;
                        datFileStruct.DataChannel = string.Join(" ", dataChannelDyn);
                        datFileStruct.Offset = xBase;

                        datFileStructsCollection.Add(datFileStruct);
                    }
                    catch
                    {
                        continue;
                    }
                }
            }
            fileReader.Close();
            return datFileStructsCollection;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="query"> Запрос для получения данных по конкретным каналам </param>
        /// <returns> List<DatFileStruct> </returns>
        List<DatFileStruct> GetDatFileStructCollection(List<QueryDatFileStruct> queryDatFileStructsCol)
        {
            // для вывода результатов запроса
            float xBase = 0;
            float xOffset = 0;
            object dataChannel;

            List<DatFileStruct> datFileStructsCollection = new List<DatFileStruct>();

            if (queryDatFileStructsCol.Count > 0)
            {
                foreach(var queryDatFileStruct in queryDatFileStructsCollection)
                {
                    IBA.ChannelID chID = new IBA.ChannelID(
                        queryDatFileStruct.ModuleId,
                        queryDatFileStruct.Channelid,
                        queryDatFileStruct.Digital);

                    var query = fileReader.QueryChannelByID(chID);
                    query.QueryData(out xBase, out xOffset, out dataChannel);


                    DatFileStruct datFileStruct = new DatFileStruct();

                    datFileStruct.ModuleId = chID.ModuleNumber;
                    datFileStruct.ModuleName = "-";
                    datFileStruct.StartDate = DateTimeToUnixTimestampFunc(fileReader.StartTime); // формат unix timestamp

                    datFileStruct.Channelid = chID.NumberInModule;
                    datFileStruct.Digital = chID.Digital;
                    datFileStruct.Unit = query.Unit;
                    datFileStruct.ChannelName = query.Name;

                    dynamic dataChannelDyn = dataChannel;
                    datFileStruct.DataChannel = string.Join(" ", dataChannelDyn);
                    datFileStruct.Offset = xBase;

                    datFileStructsCollection.Add(datFileStruct);
                }
            }
            fileReader.Close();
            return datFileStructsCollection;
        }
        
        ~DatToParquetEngine()
        {
            fileReader.Dispose();
        }
    }
}
