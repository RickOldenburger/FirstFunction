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
using System.Text;
using System.Text.RegularExpressions;

/*
    SQL server name: rickodb1.database.windows.net
    SQL Database Name: db1
    SQL User: sqluser
    SQL pwd: pswd1234!
*/

/*
Oracle bulk copy for dataTable
 OracleConnection oracleConnection = new OracleConnection(Variables.strOracleCS);

                    oracleConnection.Open();
                    using (OracleBulkCopy bulkCopy = new OracleBulkCopy(oracleConnection))
                    {
                        bulkCopy.DestinationTableName = qualifiedTableName;
                        bulkCopy.WriteToServer(dataTable);
                    }
                    oracleConnection.Close();
                    oracleConnection.Dispose();

*/

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
                            log?.LogWarning(e, "Exception: processing: row {current}, Array {ary}", 
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
                                    log?.LogWarning(e, "(case JsonToken.PropertyName) Exception: processing: row {current}, Array {arrays}",
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
        
        private List<string> _mergeOn;
        public List<string> mergeOn
        {
            get { return _mergeOn; }
            set { _mergeOn = value; }
        }

        private List<string> _mergeField;
        public List<string> mergeField
        {
            get { return _mergeField; }
            set { _mergeField = mergeField; }
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
        	return await bulkUpoad(fs, _tableName, _fieldMappings, 10000, 600, log, cancellationToken);
        }

        async public Task<int> bulkUpoad(Stream fs, 
            CancellationToken cancellationToken = default(CancellationToken))
        {
        	return await bulkUpoad(fs, _tableName, _fieldMappings, 10000, 600, null, cancellationToken);
        }

        public async Task<int> bulkUpoad(Stream fs, int batch = 10000,
            int timeOut = 600, ILogger log = null, 
            CancellationToken cancellationToken = default(CancellationToken))
        {   
		    return await bulkUpoad(fs, _tableName, _fieldMappings, 10000, 600, log, cancellationToken);
	    }

        async public Task<int> bulkUpoad(Stream fs, string tableName, int batch = 10000,
            int timeOut = 600, ILogger log = null, 
            CancellationToken cancellationToken = default(CancellationToken))
        {
        	return await bulkUpoad(fs, tableName, _fieldMappings, 10000, 600, log, cancellationToken);
        }

        async public Task<int> bulkUpoad(Stream fs, string tableName, 
            Dictionary<string, string> fieldMappings, 
            int batch = 10000, int timeOut = 600, ILogger log = null, 
            CancellationToken cancellationToken = default(CancellationToken), SqlBulkCopyOptions options = 0)
        {
            using (SqlConnection con = new SqlConnection(connStr))
            {
                try
                {
                    await con.OpenAsync(cancellationToken);
                }
                catch (Exception e)
                {
                    cancellationToken.ThrowIfCancellationRequested(); //If canceled throw an exception
                    logger.add((e, $"SqlBulkCopy open Exception"));
                    log?.LogWarning(e, "SqlBulkCopy open Exception");
                    return -1;
                }

                return (await bulkQuery(con, fs, tableName, fieldMappings, 
                    batch, timeOut, log, cancellationToken, options));
            }
        }

        async public Task<int> bulkQuery(SqlConnection con, Stream fs, 
            string tableName, Dictionary<string, string> fieldMappings,
            int batch = 10000, int timeOut = 600, ILogger log = null, 
            CancellationToken cancellationToken = default(CancellationToken),
            SqlBulkCopyOptions options = 0)
        {
            int written = 0;

            using (SqlBulkCopy objBulk = new SqlBulkCopy(con, options, null))
            {
                objBulk.BulkCopyTimeout = 600;
                objBulk.DestinationTableName = tableName;

                int prevArrays = 1;

                await foreach(DataTable dt in jsonConversions.jsonStreamTable(fs, 10000, null, cancellationToken))
                {
                    int arrays = jsonConversions.totalArrays;
                    try
                    {
                        if (objBulk.ColumnMappings.Count == 0)
                            foreach (DataColumn col in dt.Columns)
                            {
                                string value;

                                if (fieldMappings is null || !fieldMappings!.TryGetValue(col.ColumnName, out value))
                                    value = col.ColumnName;
                                if (value is not null)
                                    objBulk.ColumnMappings.Add(col.ColumnName, value);
                            }
                        try
                        {
                            await objBulk.WriteToServerAsync(dt, cancellationToken);
                            written += jsonConversions.currentCounter;
                        }
                        catch (Exception e)
                        {
                            cancellationToken.ThrowIfCancellationRequested(); //If canceled throw an exception
                            prevArrays = arrays;
                            objBulk.ColumnMappings.Clear();
                            int current = jsonConversions.currentCounter + jsonConversions.currentBlockCount;

                            logger.add((e, $"SqlBulkCopy failed Exception: row {current}, Array {arrays}"));
                            log?.LogWarning(e, "SqlBulkCopy failed Exception: row {current}, Array {arrays}", current, arrays);
                        }
                        finally 
                        {
                            if (prevArrays != arrays)
                            {
                                prevArrays = arrays;
                                objBulk.ColumnMappings.Clear();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        cancellationToken.ThrowIfCancellationRequested(); //If canceled throw an exception
                        logger.add((e, "ColumnMappings failed Exception"));
                        log?.LogWarning(e, "ColumnMappings failed Exception");
                    }
                }
            }

            return written;
        }
    public async Task<Tuple<int, int>> bulkMerge(Stream fs, string tableName,
            Dictionary<string, string> fieldMappings, 
            List<string> mergeOn, List<string> mergeField = null,
            bool purgeOmitted = false,
            int batch = 10000, int timeOut = 600, ILogger log = null, 
            CancellationToken cancellationToken = default(CancellationToken))
        {
            int written = -1, updated = -1;

            using (SqlConnection con = new SqlConnection(connStr))
            {
                
                if (mergeOn?.Count > 0)
                {
                    try
                    {
                        await con.OpenAsync(cancellationToken);

                        string wkTableName = TableTools.removeInvalidChars(tableName);
                        //create/replace temporary table for our merge query
                        string sqlQuery = SqlStrings.buildTempTable.Replace("{tableName}", wkTableName);
                        SqlCommand cmd = new SqlCommand(sqlQuery, con);
                        await cmd.ExecuteNonQueryAsync(cancellationToken);

                        //Write Json stream to newly created temp table
                        written = await bulkQuery(con, fs, "#"+tableName, fieldMappings, 
                            batch, timeOut, log, cancellationToken, SqlBulkCopyOptions.TableLock);

                        /* 
                        This code is for debugging purposes and will show us the first 10 records loaded as well as a 
                        total count from the temp table. This may be important since there is no easy way to view the 
                        data that is loaded into this table

                        DataTable dt = new DataTable();
                        sqlQuery = "SELECT top 10 a.*, count(*) over () FROM [" + wkTableName + "] a";
                        SqlCommand command = new SqlCommand(sqlQuery, con);
                        SqlDataAdapter da = new SqlDataAdapter(command);
                        da.Fill(dt);
                        TableTools.logTable(dt); 
                        */

                        //Merge newly created temp table with existing table
                        int offset = 0;
                        StringBuilder sb = new StringBuilder(
                            SqlStrings.mergeTempTableStart
                            .Replace("{tableName}", wkTableName)
                            .Replace("{mergeOnTB}", TableTools.buildTableInsert(mergeOn, "@onTB", ref offset))
                            .Replace("{insertFieldTB}", TableTools.buildTableInsert(mergeField, "@fieldTB", ref offset)));
                        if (purgeOmitted)
                            sb.Append(SqlStrings.mergeTempTablePurge);
                        else
                            sb.Append(SqlStrings.mergeTempTableNoPurge);
                        sb.Append(SqlStrings.mergeTempTableEnd);

                        /* 
                        This will log the full merge query for analysis. This may be important since the merge query 
                        is dynamically created.
                        
                        log?.LogWarning(sb.ToString()); 
                        */

                        cmd = new SqlCommand(sb.ToString(), con);
                        cmd.CommandTimeout = timeOut;
                        updated = await cmd.ExecuteNonQueryAsync(cancellationToken) - offset;
                    }
                    catch (Exception e)
                    {
                        cancellationToken.ThrowIfCancellationRequested(); //If canceled throw an exception
                        logger.add((e, "bulkMerge open Exception"));
                        log?.LogWarning(e, "bulkMerge open Exception");
                    }
                }
                else
                {
                    ArgumentException e = new ArgumentNullException("Argument List<string> null Exception");
                    logger.add((e, "Argument List<string>: mergeOn must contain at least one entry"));
                    log?.LogWarning(e, "Argument List<string>: mergeOn must contain at least one entry");
                }
            }

            return Tuple.Create(written, updated);
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
        private const string ValidateTable = @"[^A-Z0-9@#$]";
        private static readonly Regex rxTbl = new Regex(ValidateTable, RegexOptions.Compiled|RegexOptions.IgnoreCase);

        public static string removeInvalidChars(string s) { return rxTbl.Replace(s, "");}

        public static string buildTableInsert(List<string> lst, string tb, ref int cntr)
        {
            StringBuilder wrkBldr = new StringBuilder();
            int flds = lst?.Count ?? 0;
            if (flds>0)
            {
                lst[0] = TableTools.removeInvalidChars(lst[0]);

                wrkBldr.Append("insert into ")
                       .Append(tb)
                       .Append(" values('")
                       .Append(lst.Aggregate((x, y) => 
                            x + "'),('" + TableTools.removeInvalidChars(y)))
                       .Append("');");
                cntr+=flds;
            }
            return wrkBldr.ToString();
        }

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