using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Data;
using System;
using System.IO;
using Microsoft.Extensions.Logging;

namespace JsonTools
{
    public static class JsonConversions
    {
        // convert Json strings to datatable
        public static DataTable stringToTable(string s, ILogger log)
        {
            try
            {
                return (DataTable)JsonConvert.DeserializeObject(forceArray(s), (typeof(DataTable)));
            }
            catch (Exception e)
            {
                log.LogError(e.ToString());
                return (new DataTable());
            }
        }

        public static void jsonStreamToTable(out DataTable dt, Stream fs, ILogger log)
        {
            using (StreamReader sr = new StreamReader(fs))
                using(JsonReader reader = new JsonTextReader(sr))
                {
                    int ary = 0, cnt = 0;

                    dt = new DataTable();
                    DataRow rw = null;

                    try
                    {
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
                                        log.LogError(e.ToString());
                                    }
                                    break;
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        log.LogError(e.ToString());
                    }
                }
        }

        public static string forceArray(string s)
        {
            return isArray(s) ? s : "[" + s + "]";
        }

        public static bool isArray(string s)
        {
            JToken token = JToken.Parse(s);
            return (token is Newtonsoft.Json.Linq.JArray);
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
