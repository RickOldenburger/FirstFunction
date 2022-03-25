using System;
using System.Data;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;
using System.Data.SqlClient;
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
                            logger.add((e, $"Exception: processing: row {current}, Array {_totalArrays}"));
                            log?.LogWarning(e, "Exception: processing: row {current}, Array {ary}", 
                                current, _totalArrays);
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
                                    _currentCounter = 0;
                                    _currentBlockCount = 0;
                                    yield return dt;
                                    dt.Clear();
                                    dt.Columns.Clear();
                                }

                                break;
                            case JsonToken.StartObject:
                                if (batch > 0 && _currentCounter >= batch)
                                {
                                    _currentBlockCount += _currentCounter;
                                    _totalBlocksCount += _currentCounter;
                                    _currentCounter = 0;
                                    yield return dt;
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
                                    logger.add((e, $"(case JsonToken.PropertyName) processing: row {current}, Array {_totalArrays}"));
                                    log?.LogWarning(e, "(case JsonToken.PropertyName) Exception: processing: row {current}, Array {ary}",
                                        current, _totalArrays);
                                }
                                break;
                        }
                    }
                }

            if (_currentCounter > 0)
            {
                _currentBlockCount += _currentCounter;
                _totalBlocksCount += _currentCounter;
                _currentCounter = 0;
                yield return dt;
            }

        }
    }

    public class SQLProcess
    {
        internal Logger logger;
        internal JsonConversions jsonConversions;
        private string connStr;

        private string _tableName;
        public string tableName 
        {
            get { return _tableName; }
            set { _tableName = value; }
        }

        private Dictionary<string, string> _fieldMappings;
        public Dictionary<string, string> fieldMappings 
        {
            get { return _fieldMappings;}
            set { _fieldMappings = value; }
        }
        
        public SQLProcess(string tableName = null, Dictionary<string, string> fieldMappings = null)
        {
            logger = new Logger();
            jsonConversions = new JsonConversions(logger);
            _tableName = tableName;
            _fieldMappings = fieldMappings;
            connStr = connect();
        }

        async public Task<int> bulkUpoad(Stream fs, ILogger log = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return await bulkUpoad(fs, 10000, 600, log, cancellationToken);
        }

        async public Task<int> bulkUpoad(Stream fs, 
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return await bulkUpoad(fs, 10000, 600, null, cancellationToken);
        }
        async public Task<int> bulkUpoad(Stream fs, string tableName, 
            Dictionary<string, string> fieldMappings, 
            int timeOut = 600, int batch = 10000, ILogger log = null, 
            CancellationToken cancellationToken = default(CancellationToken))
        {
            _fieldMappings = fieldMappings;
            return await bulkUpoad(fs, tableName, timeOut, batch, log, cancellationToken);
        }

        async public Task<int> bulkUpoad(Stream fs, string tableName, int batch = 10000,
            int timeOut = 600, ILogger log = null, 
            CancellationToken cancellationToken = default(CancellationToken))
        {
            _tableName = tableName;
            return await bulkUpoad(fs, timeOut, batch, log, cancellationToken);
        }

        public async Task<int> bulkUpoad(Stream fs, int batch = 10000,
            int timeOut = 600, ILogger log = null, 
            CancellationToken cancellationToken = default(CancellationToken))
        {   
            int written = 0;

            using (SqlConnection con = new SqlConnection(connStr))
            {
                con.Open();

                using (SqlBulkCopy objBulk = new SqlBulkCopy(con))
                {
                    objBulk.BulkCopyTimeout = 600;
                    objBulk.DestinationTableName = _tableName;

                    await foreach(DataTable dt in jsonConversions.jsonStreamTable(fs, 10000, null, cancellationToken))
                    {
                        if (objBulk.ColumnMappings.Count == 0)
                            foreach (DataColumn col in dt.Columns)
                            {
                                string value = "";
                                string colName = col.ColumnName;
                                if (_fieldMappings is null || !_fieldMappings!.TryGetValue(colName, out value))
                                    value = col.ColumnName;
                                if (value is not null)
                                    objBulk.ColumnMappings.Add(colName, value);
                            }
                        
                        await objBulk.WriteToServerAsync(dt, cancellationToken);
                        //written += objBulk.SqlRowsCopied
                    }
                }
            }

            return written;
        }
        
        public static string connect()
        {
            string connStr = Environment.GetEnvironmentVariable("connection_string", EnvironmentVariableTarget.Process);
            string password = Environment.GetEnvironmentVariable("CustomCONNSTR_password", EnvironmentVariableTarget.Process);
            string user = Environment.GetEnvironmentVariable("CustomCONNSTR_user", EnvironmentVariableTarget.Process);

            return connStr.Replace("{user}", user).Replace("{password}", password);
        }
    }

    internal class Logger
    {
        private List<(Exception, string)> _messages;
        private int _messagePos;

        public List<(Exception, string)> messages{ get {return _messages;} }
        public int messagePos{ get {return _messagePos;} }

        public Logger()
        {
            _messages = new List<(Exception, string)>();
            _messagePos = 0;
        }

        public void clearMessages()
        {
            _messages.Clear();
            _messagePos = 0;
        }

        public void resetMessage()
        {
            _messagePos = 0;
        }

        public IEnumerable<(Exception, string)> getNextMessage()
        {
            while (_messages?.Count() > _messagePos)
                yield return _messages[_messagePos++];
        }

        internal void add((Exception, string) message)
        {
            _messages.Add(message);
        }
    }
    public static class TableTools
    {
        public static void logTable(DataTable dt, bool HeadersOnly = false)
        {
            DataRow[] currentRows = dt.Select(null, null, DataViewRowState.CurrentRows);

            if (currentRows.Length < 1 )
                Console.WriteLine("No Current Rows Found");
            else
            {
                foreach (DataColumn column in dt.Columns)
                    Console.Write("\t{0}", column.ColumnName);

                if (HeadersOnly) return;

                Console.WriteLine("\tRowState");

                foreach (DataRow row in currentRows)
                {
                    foreach (DataColumn column in dt.Columns)
                    Console.Write("\t{0}", row[column]);

                    Console.WriteLine("\t" + row.RowState);
                }
            }
        }
    }

}