using System;
using System.Data;
using System.IO;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Data.SqlClient;
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
    public class SQLProcess
    {
        // General fields for the class
        internal Logger logger;
        internal JsonConversions jsonConversions;
        private SQLConnection sqlConnection;

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
            sqlConnection = new SQLConnection();
            sqlConnection.connect();
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
            using (SqlConnection con = new SqlConnection(sqlConnection.connection))
            {
                try
                {
                    await con.OpenAsync(cancellationToken);
                }
                catch (Exception e)
                {
                    cancellationToken.ThrowIfCancellationRequested(); //If canceled throw an exception
                    logger.add((e, $"SqlBulkCopy open Exception"));
                    log?.LogError(e, "SqlBulkCopy open Exception");
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
                            log?.LogError(e, "SqlBulkCopy failed Exception: row {current}, Array {arrays}", current, arrays);
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
                        log?.LogError(e, "ColumnMappings failed Exception");
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

            using (SqlConnection con = new SqlConnection(sqlConnection.connection))
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
                        log?.LogError(e, "bulkMerge open Exception");
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

        public static void putData(ref int cnt, int jsonRows, object[] ytls, ILogger log, 
        CancellationToken cancellationToken)
        {
        
        }
    }
}