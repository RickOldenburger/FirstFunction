using System;
using System.Data;
using System.IO;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;

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
    /// <summary>
    /// Class <c>SQLProcess</c> encapsulates properties and methods allowing for interrogating
    /// and updating backend SQL tables with JSON
    /// </summary>
    public class SQLProcess
    {
        /// <summary>
        /// property <c>Logger</c> Internal property that if populated will generate logs 
        /// for any warnings or exceptions encountered
        /// </summary>
        internal Logger logger;

        /// <summary>
        /// property <c>JsonConversions</c>: internal property encapsulates JSON conversions 
        /// and is utilized for parsing JSON text into tables
        /// </summary>
        internal JsonConversions jsonConversions;

        /// <summary>
        /// property <c>SQLConnection</c>: private property that is the SQL connection 
        /// </summary>
        private SQLConnection sqlConnection;

        private string _tableName;

        /// <summary>
        /// property <c>tableName</c>: contains the physical table that will be processed. 
        /// This is assignment is optional and can be overridden at the function call level.
        /// </summary>
        public string tableName 
        {
            get { return _tableName; }
            set { _tableName = value; }
        }

        private Dictionary<string, string> _fieldMappings;

        /// <summary>
        ///  property <c>fieldMappings</c>: This is a cross reference for mapping to backend SQL tables.
        /// <para>
        ///  This property contains the field mappings that are interrogated to determine a cross
        ///  reference value for inserting values into a backend table. If the cross reference value
        ///  is null then the value will not be loaded.
        /// </para>
        /// <example>
        ///  For example:
        /// <list type="bullet">
        /// <item>
        ///  <description>{&quot;namester&quot;, &quot;name&quot;} will Map a JSON value <c>namester</c> to <c>name</c> at the SQL level</description>
        /// </item>
        /// <item>
        ///  <description>{&quot;cntr&quot;, null} will drop the JSON value <c>cntr</c></description>
        /// </item>
        /// </list>
        /// </example>
        /// </summary>
        public Dictionary<string, string> fieldMappings 
        {
            get { return _fieldMappings;}
            set { _fieldMappings = value; }
        }
        
        private List<string> _mergeOn;

        /// <summary>
        /// property <c>mergeOn</c>: is a list of fields that will be used as criteria
        /// for merging records when the merge function is used. These rows within the Jason must 
        /// be unqiue.
        /// </summary>
        public List<string> mergeOn
        {
            get { return _mergeOn; }
            set { _mergeOn = value; }
        }

        private List<string> _mergeFields;

        /// <summary>
        /// property <c>mergeFields</c>: is a list of fields that will be updated or inserted
        /// into the target table. 
        /// </summary>
        public List<string> mergeFields
        {
            get { return _mergeFields; }
            set { _mergeFields = mergeFields; }
        }

        /// <summary>
        ///  This class will establish a connection to the backend database and then execute 
        ///  a bulk insert into the table.
        /// </summary>
        /// <param name="tableName">Contains the physical table that will be processed.</param>
        /// <param name="fieldMappings">This is a cross reference for mapping to backend SQL tables.</param>
        /// <param name="mergeOn">List of fields that will be used as criteria for merging records when 
        /// the merge function is used.</param>
        /// <param name="mergeFields">List of fields that will be updated or inserted into the target table.</param>
        public SQLProcess(string tableName = null, Dictionary<string, string> fieldMappings = null,
            List <string> mergeOn = null, List<string> mergeFields = null)
        {
            logger = new Logger();
            jsonConversions = new JsonConversions(logger);
            _tableName = tableName;
            _fieldMappings = fieldMappings;
            _mergeOn = mergeOn;
            _mergeFields = mergeFields;
            sqlConnection = new SQLConnection();
        }

        /// <summary>
        ///  This class will establish a connection to the backend database and then execute 
        ///  a bulk insert into the table.
        /// </summary>
        /// <param name="fs">This is a filestream that needs to be in JSON fromat. Arrays of JSON data
        ///  is supported.</param>
        /// <param name="log">Optional parm that specifies a logger that will generate logs for any
        ///  warnings or exceptions encountered</param>
        async public Task<int> bulkUpoad(Stream fs, 
            CancellationToken cancellationToken = default(CancellationToken))
        {
        	return await bulkInsert(fs, _tableName, _fieldMappings, 10000, 600, null, cancellationToken);
        }

        /// <summary>
        ///  This class will establish a connection to the backend database and then execute 
        ///  a bulk insert into the table.
        /// </summary>
        /// <param name="fs">This is a filestream that needs to be in JSON fromat. Arrays of JSON data
        ///  is supported.</param>
        /// <param name="log">Optional parm that specifies a logger that will generate logs for any
        ///  warnings or exceptions encountered</param>
        /// <param name="cancellationToken">Optional parm that allows passing a cancellation token.</param>
        async public Task<int> bulkUpoad(Stream fs, ILogger log = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
        	return await bulkInsert(fs, _tableName, _fieldMappings, 10000, 600, log, cancellationToken);
        }

        /// <summary>
        ///  This class will establish a connection to the backend database and then execute 
        ///  a bulk insert into the table.
        /// </summary>
        /// <param name="fs">This is a filestream that needs to be in JSON fromat. Arrays of JSON data
        ///  is supported.</param>
        /// <param name="batch">This specifies the blocksize of records sets that are processed at a time.
        ///  Limiting the block size will conserve server side memory.</param>
        /// <param name="timeOut">This specifies the maximum time, in seconds, a particular SQL execution will be permitted
        ///  to take. It should be noted that the longest a funciton can execute is 30 minutes.</param>
        /// <param name="log">Optional parm that specifies a logger that will generate logs for any
        ///  warnings or exceptions encountered</param>
        /// <param name="cancellationToken">Optional parm that allows passing a cancellation token.</param>
        public async Task<int> bulkUpoad(Stream fs, int batch = 10000,
            int timeOut = 600, ILogger log = null, 
            CancellationToken cancellationToken = default(CancellationToken))
        {   
		    return await bulkInsert(fs, _tableName, _fieldMappings, 10000, 600, log, cancellationToken);
	    }

        /// <summary>
        ///  This class will establish a connection to the backend database and then execute 
        ///  a bulk insert into the table.
        /// </summary>
        /// <param name="fs">This is a filestream that needs to be in JSON fromat. Arrays of JSON data
        ///  is supported.</param>
        /// <param name="tableName">Contains the physical table that will be processed.</param>
        /// <param name="batch">This specifies the blocksize of records sets that are processed at a time.
        ///  Limiting the block size will conserve server side memory.</param>
        /// <param name="timeOut">This specifies the maximum time, in seconds, a particular SQL execution will be permitted
        ///  to take. It should be noted that the longest a funciton can execute is 30 minutes.</param>
        /// <param name="log">Optional parm that specifies a logger that will generate logs for any
        ///  warnings or exceptions encountered</param>
        /// <param name="cancellationToken">Optional parm that allows passing a cancellation token.</param>
        async public Task<int> bulkUpoad(Stream fs, string tableName, int batch = 10000,
            int timeOut = 600, ILogger log = null, 
            CancellationToken cancellationToken = default(CancellationToken))
        {
        	return await bulkInsert(fs, tableName, _fieldMappings, 10000, 600, log, cancellationToken);
        }

        /// <summary>
        ///  This class will establish a connection to the backend database and process the filestream
        ///  a bulk insert into the table.
        /// </summary>
        /// <param name="fs">This is a filestream that needs to be in JSON fromat. Arrays of JSON data
        ///  is supported.</param>
        /// <param name="tableName">Contains the physical table that will be processed.</param>
        /// <param name="fieldMappings">This is a cross reference for mapping to backend SQL tables.</param>
        /// <param name="batch">This specifies the blocksize of records sets that are processed at a time.
        ///  Limiting the block size will conserve server side memory.</param>
        /// <param name="timeOut">This specifies the maximum time, in seconds, a particular SQL execution will be permitted
        ///  to take. It should be noted that the longest a funciton can execute is 30 minutes.</param>
        /// <param name="log">Optional parm that specifies a logger that will generate logs for any
        ///  warnings or exceptions encountered</param>
        /// <param name="cancellationToken">Optional parm that allows passing a cancellation token.</param>
        async public Task<int> bulkInsert(Stream fs, string tableName, 
            Dictionary<string, string> fieldMappings, 
            int batch = 10000, int timeOut = 600, ILogger log = null, 
            CancellationToken cancellationToken = default(CancellationToken), SqlBulkCopyOptions options = 0)
        {
            using (SqlConnection con = new SqlConnection(sqlConnection.connection))
            {
                try
                {
                    await con.OpenAsync(cancellationToken);
                }
                catch (Exception e)
                {
                    logger.add((e, $"SqlBulkCopy open Exception"));
                    log?.LogError(e, "SqlBulkCopy open Exception");
                    cancellationToken.ThrowIfCancellationRequested(); //If canceled throw an exception
                    return -1;
                }

                return (await bulkQuery(con, fs, tableName, fieldMappings, 
                    batch, timeOut, log, cancellationToken, options));
            }
        }

        /// <summary>
        ///  This private class will take an open connection and parse a stream file
        ///  This stream contains either an aray of JSON (Or a single entry) into a 
        ///  DataTable. This table is then bulk inserted into the tableName field
        /// </summary>
        /// <param name="con">A Connection to sql Server that must be connected.</param>
        /// <param name="fs">This is a filestream that needs to be in JSON fromat. Arrays of JSON data
        ///  is supported.</param>
        /// <param name="tableName">Contains the physical table that will be processed.</param>
        /// <param name="fieldMappings">This is a cross reference for mapping to backend SQL tables.</param>
        /// <param name="batch">This specifies the blocksize of records sets that are processed at a time.
        ///  Limiting the block size will conserve server side memory.</param>
        /// <param name="timeOut">This specifies the maximum time, in seconds, a particular SQL execution
        ///  will be permitted to take. It should be noted that the longest a funciton can execute is 
        ///  30 minutes.</param>
        /// <param name="log">Optional parm that specifies a logger that will generate logs for any
        ///  warnings or exceptions encountered</param>
        /// <param name="cancellationToken">Optional parm that allows passing a cancellation token.</param>
        /// <param name="options">SqlBulkCopyOptions will permit passing options that are passed directly
        ///  to the bulkinsert function.</param>
        async private Task<int> bulkQuery(SqlConnection con, Stream fs, 
            string tableName, Dictionary<string, string> fieldMappings,
            int batch = 10000, int timeOut = 600, ILogger log = null, 
            CancellationToken cancellationToken = default(CancellationToken),
            SqlBulkCopyOptions options = 0)
        {
            int written = 0;

            try
            {
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
                                prevArrays = arrays;
                                objBulk.ColumnMappings.Clear();
                                int current = jsonConversions.currentCounter + jsonConversions.currentBlockCount;

                                logger.add((e, $"SqlBulkCopy failed Exception (WriteToServerAsync): row {current}, Array {arrays}"));
                                log?.LogError(e, "SqlBulkCopy failed Exception (WriteToServerAsync): row {current}, Array {arrays}", current, arrays);
                                cancellationToken.ThrowIfCancellationRequested(); //If canceled throw an exception
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
                            logger.add((e, "SqlBulkCopy failed Exception (ColumnMappings): failed Exception"));
                            log?.LogError(e, "SqlBulkCopy failed Exception (ColumnMappings): failed Exception");
                            cancellationToken.ThrowIfCancellationRequested(); //If canceled throw an exception
                        }
                    }
                }
            }
            catch (Exception e)
            {
                logger.add((e, "SqlBulkCopy failed Exception (connnection): failed Exception"));
                log?.LogError(e, "SqlBulkCopy failed Exception (connnection): failed Exception");
                cancellationToken.ThrowIfCancellationRequested(); //If canceled throw an exception
            }

            return written;
        }

        /// <summary>
        ///  This class will perform a bulk merge to a specified table the records found in a JSON
        ///  stream.<br /> This stream contains an aray of JSON (or a single JSON entry) that will be merged 
        ///  into a temporary table on the SQL server.<br />This temporary table will then be merged into the 
        ///  <c>tableName</c> that is passed to the function.<br />The <c>mergeOn</c> list will be used to 
        ///  determine which record(s) are updated on the SQL table.<br />Note: the record entries specified in the
        ///  JSON file must only contain unique values based on the <c>mergeOn</c> field.
        /// </summary>
        /// <param name="fs">This is a filestream that needs to be in JSON fromat. Arrays of JSON data
        ///  is supported.</param>
        /// <param name="tableName">Contains the physical table that will be processed.</param>
        /// <param name="fieldMappings">This is a cross reference for mapping to backend SQL tables.</param>
        /// <param name="mergeOn">List of fields that will be used as criteria for merging
        ///  records when the merge function is used. These rows within the Jason must be unqiue.</param>
        /// <param name="mergeField">List of fields that will be updated or inserted into the target table.</param>
        /// <param name="batch">This specifies the blocksize of records sets that are processed at a time.
        ///  Limiting the block size will conserve server side memory.</param>
        /// <param name="timeOut">This specifies the maximum time, in seconds, a particular SQL execution
        ///  will be permitted to take. It should be noted that the longest a funciton can execute is 
        ///  30 minutes.</param>
        /// <param name="log">Optional parm that specifies a logger that will generate logs for any
        ///  warnings or exceptions encountered</param>
        /// <param name="cancellationToken">Optional parm that allows passing a cancellation token.</param>
        public async Task<Tuple<int, int>> bulkMerge(Stream fs, string tableName,
            Dictionary<string, string> fieldMappings, 
            List<string> mergeOn, List<string> mergeField = null,
            bool purgeOmitted = false,
            int batch = 10000, int timeOut = 600, ILogger log = null, 
            CancellationToken cancellationToken = default(CancellationToken))
        {
            int written = -1, updated = -1;

            using (SqlConnection con = new SqlConnection(sqlConnection.connection))
            {
                
                if (mergeOn?.Count > 0)
                {
                    try
                    {
                        await con.OpenAsync(cancellationToken);

                        string wkTableName = TableTools.removeInvalidChars(tableName);
                        //create/replace temporary table for our merge query
                        string sqlQuery = SqlStrings.buildTempTable.Replace("{tableName}", wkTableName)
                            .Replace("{onTB}", TableTools.buildFromList(mergeOn));
                        SqlCommand cmd = new SqlCommand(sqlQuery, con);
                        await cmd.ExecuteNonQueryAsync(cancellationToken);

                        //Write Json stream to newly created temp table
                        written = await bulkQuery(con, fs, "#"+tableName, fieldMappings, 
                            batch, timeOut, log, cancellationToken, SqlBulkCopyOptions.TableLock);

                        // /* 
                        // This code is for debugging purposes and will show us the first 10 records loaded as well as a 
                        // total count from the temp table. This may be important since there is no easy way to view the 
                        // data that is loaded into this table

                        DataTable dt = new DataTable();
                        sqlQuery = "SELECT top 10 a.*, count(*) over () FROM [#" + wkTableName + "] a";
                        SqlCommand command = new SqlCommand(sqlQuery, con);
                        SqlDataAdapter da = new SqlDataAdapter(command);
                        da.Fill(dt);
                        TableTools.logTable(dt); 
                        // */

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

                        // /* 
                        // This will log the full merge query for analysis. This may be important since the merge query 
                        // is dynamically created.
                        
                        Console.Write(sb.ToString()); 
                        // */

                        cmd = new SqlCommand(sb.ToString(), con);
                        cmd.CommandTimeout = timeOut;
                        updated = await cmd.ExecuteNonQueryAsync(cancellationToken) - offset;
                    }
                    catch (Exception e)
                    {
                        logger.add((e, "bulkMerge open Exception"));
                        log?.LogError(e, "bulkMerge open Exception");
                        cancellationToken.ThrowIfCancellationRequested(); //If canceled throw an exception
                    }
                }
                else
                {
                    ArgumentException e = new ArgumentNullException("Argument List<string> null Exception");
                    logger.add((e, "Argument List<string>: mergeOn must contain at least one entry"));
                    log?.LogError(e, "Argument List<string>: mergeOn must contain at least one entry");
                }
            }

            return Tuple.Create(written, updated);
        }

        /// <summary>
        ///  This class will perform an insert into the <c>tableName</c>. This insert is only meant for small data sets.
        ///  <br /> The <c>table</c> is then inseted into the backend database.
        /// </summary>
        /// <param name="tableName">Contains the physical table that will be processed.</param>
        /// <param name="table">This is an array of classes that is meant to</param>
        /// <param name="log">Optional parm that specifies a logger that will generate logs for any
        ///  warnings or exceptions encountered</param>
        /// <param name="cancellationToken">Optional parm that allows passing a cancellation token.</param>

        public async Task<int> putData(string tableName, object[] table, ILogger log, 
            CancellationToken cancellationToken)
        {
            int cnt = -1;

            try
            {
                if ((table?.Length ?? 0) > 0)
                {
                    PropertyInfo[] objsInfo = table[0].GetType().GetProperties();
                    List<string> flds = objsInfo.Select(p => p.Name).ToList();
                    string sqlString = TableTools.buildTableParms(flds, tableName);

                    using (SqlConnection con = new SqlConnection(sqlConnection.connection))
                    {
                        await con.OpenAsync(cancellationToken);
                        SqlCommand command = new SqlCommand(sqlString, con);
                        foreach (object tableEntry in table)
                        {
                            cancellationToken.ThrowIfCancellationRequested(); //If canceled throw an exception
                            PropertyInfo[] tableInfo = tableEntry.GetType().GetProperties();

                            foreach (PropertyInfo tableInf in tableInfo)
                            {
                                dynamic val = tableInf.GetValue(tableEntry);
                                command.Parameters.AddWithValue(tableInf.Name, val);
                            }

                            cnt = await command.ExecuteNonQueryAsync(cancellationToken);
                            command.Parameters.Clear();
                            cnt++;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                logger.add((e, "putData: Error processing function."));
                log?.LogError(e, "putData: Error processing function.");
                cancellationToken.ThrowIfCancellationRequested(); //If canceled throw an exception
            }

            return cnt;
        }
    }
}