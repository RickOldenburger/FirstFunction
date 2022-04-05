using System;
using System.Data;
using System.IO;
using System.Threading;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace JsonTools
{    
    public class JsonConversions
    {
        //Error messages
        internal Logger logger;

        public JsonConversions()
        {
            logger = new Logger();
        }

        //Enable linking a different logger to this class
        internal JsonConversions(Logger lgr)
        {
            logger = lgr;
        }
        //Tables processed
        private int _totalArrays;
        private int _totalBlocksCount;
        private int _currentBlockCount;
        private int _currentCounter;
        
        public int totalArrays { get { return _totalArrays; } }
        public int totalBlocksCount {get { return _totalBlocksCount; }}
        public int currentBlockCount { get { return _currentBlockCount; } }
        public int currentCounter { get { return _currentCounter; } }

        public static string forceArray(string s)
        {
            return isArray(s) ? s : "[" + s + "]";
        }

        public static bool isArray(string s)
        {
            JToken token = JToken.Parse(s);
            return (token is Newtonsoft.Json.Linq.JArray);
        }

        // convert Json strings to datatable
        public DataTable jsonStringToTable(string s, ILogger log = null)
        {
            _totalArrays = _totalBlocksCount = _currentBlockCount = _currentCounter = 0;

            try
            {
                DataTable dt = (DataTable) JsonConvert.DeserializeObject(forceArray(s), (typeof(DataTable)));
                _totalArrays = 1;
                _totalBlocksCount = _currentBlockCount = dt.Rows.Count;
                return dt;
            }
            catch (Exception e)
            {
                logger.add((e, "Failed to process: stringToTable"));
                log?.LogError(e, "Failed to process: stringToTable");
                return (new DataTable());
            }
        }

        public static void jsonFullStreamToTable(out DataTable dt, Stream fs, ILogger log)
        {
            dt = new DataTable();

            try
            {
            using (StreamReader sr = new StreamReader(fs))
                using(JsonReader reader = new JsonTextReader(sr))
                {
                    int ary = 0, cnt = 0;

                    dt = new DataTable();
                    DataRow rw = null;

                        while (reader.Read())
                        {
                            switch(reader.TokenType)
                            {
                                case JsonToken.StartArray:
                                    ary++;
                                    cnt=-1;
                                    break;
                                case JsonToken.StartObject:
                                    rw = dt.NewRow();
                                    cnt++;
                                    break;
                                case JsonToken.EndObject:
                                    dt.Rows.Add(rw);
                                    break;
                                case JsonToken.PropertyName:
                                    try
                                    {
                                        string fld = (string)reader.Value;
                                        if (!dt.Columns.Contains(fld))
                                        {
                                            DataColumn col = new DataColumn(fld);
                                            col.DataType = reader.ValueType;
                                            dt.Columns.Add(col);
                                        }
                                        
                                        if (reader.Read())
                                        {
                                            rw[fld] = Convert.ChangeType(reader.Value,reader.ValueType);
                                            //dicts[cnt].Add(fld, Convert.ChangeType(reader.Value,reader.ValueType));
                                        }
                                    }
                                    catch (Exception e)
                                    {
                                        log.LogWarning(e, "warning on row: {cnt}", cnt);
                                    }
                                    break;
                            }
                        }
                    }

                }
                catch (Exception e)
                {
                    log.LogError(e, "failed to create table");
                }
        }

        public async IAsyncEnumerable<DataTable> jsonStreamTable(Stream fs, int batch = 10000,
             ILogger log = null, 
             [EnumeratorCancellation] CancellationToken cancellationToken = default(CancellationToken))
        {
            DataTable dt = new DataTable();

            using (StreamReader sr = new StreamReader(fs))
                using(JsonReader reader = new JsonTextReader(sr))
                {
                    DataRow rw = null;
                    bool result = true;
                    _totalArrays = _totalBlocksCount = _currentBlockCount = _currentCounter = 0;

                    while (result)
                    {
                        cancellationToken.ThrowIfCancellationRequested(); //Since it is a cancel
                        result = false;
                        try
                        {
                            result = await reader.ReadAsync(cancellationToken);
                        }
                        catch (Exception e)
                        {
                            int current = _currentCounter + _currentBlockCount;
                            int arrays = _totalArrays;

                            logger.add((e, $"Exception: processing: row {current}, Array {arrays}"));
                            log?.LogError(e, "Exception: processing: row {current}, Array {ary}", 
                                current, arrays);
                            continue; // leave the while loop
                        }

                        if (!result) 
                            continue; //Simply go through the logic of the rest of the code to process the last record
                        
                        switch(reader.TokenType)
                        {
                            case JsonToken.StartArray:
                                _totalArrays += 1;
                                if (_currentCounter > 0)
                                {
                                    _totalBlocksCount += _currentCounter;
                                    yield return dt;
                                    _currentCounter = 0;
                                    _currentBlockCount = 0;
                                    dt.Clear();
                                    dt.Columns.Clear();
                                }

                                break;
                            case JsonToken.StartObject:
                                if (batch > 0 && _currentCounter >= batch)
                                {
                                    _currentBlockCount += _currentCounter;
                                    _totalBlocksCount += _currentCounter;
                                    yield return dt;
                                    _currentCounter = 0; 
                                    dt.Clear();
                                }

                                rw = dt.NewRow();
                                _currentCounter++;
                                break;
                            case JsonToken.EndObject:
                                dt.Rows.Add(rw);
                                break;
                            case JsonToken.PropertyName:
                                try
                                {
                                    string fld = (string)reader.Value;
                                    if (!dt.Columns.Contains(fld))
                                    {
                                        DataColumn col = new DataColumn(fld);
                                        col.DataType = reader.ValueType;
                                        dt.Columns.Add(col);
                                    }

                                    result = await reader.ReadAsync(cancellationToken);
                                    if (result)
                                         rw[fld] = Convert.ChangeType(reader.Value,reader.ValueType);

                                }
                                catch (Exception e)
                                {
                                    int current = _currentCounter + _currentBlockCount;
                                    int arrays = _totalArrays;

                                    logger.add((e, $"(case JsonToken.PropertyName) processing: row {current}, Array {arrays}"));
                                    log?.LogError(e, "(case JsonToken.PropertyName) Exception: processing: row {current}, Array {arrays}",
                                        current, arrays);
                                }
                                break;
                        }
                    }
                }

            if (_currentCounter > 0)
            {
                _currentBlockCount += _currentCounter;
                _totalBlocksCount += _currentCounter;
                yield return dt;
            }

        }
    }
}