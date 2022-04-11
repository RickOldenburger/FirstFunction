using System;
using System.Text;
using System.Linq;
using System.Data;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace JsonTools
{
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

        public static string buildFromList(List<string> lst)
        {
            // Returns list of fields in paranthesis
            // Ex: (fld1, fld2, fld3)
            StringBuilder wrkBldr = new StringBuilder();
            int flds = lst?.Count ?? 0;
            if (flds>0)
            {
                lst[0] = TableTools.removeInvalidChars(lst[0]);

                wrkBldr.Append("(")
                    .Append(lst.Aggregate((x, y) => 
                        x + "," + TableTools.removeInvalidChars(y)))
                    .Append(")");
            }
            return wrkBldr.ToString();
        }

        public static string buildTableInsert(List<string> lst, string tb, ref int cntr)
        {
            // Returns list of field with each in a paranthesis
            // Ex: insert into tbl1 values('fld1'),('fld2'),('fld3');
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

        public static string buildTableParms(List<string> lst, string tb)
        {   
            // Returns list of field as a parameter
            // Ex: insert into  tbl1(fld1,fld2,fld3) values(@fld1,@fld2,@fld3);       
            if ((lst?.Count ?? 0)>0)
            {
                lst[0] = TableTools.removeInvalidChars(lst[0]);

                StringBuilder wrkBldr = new StringBuilder(
                    lst.Aggregate((x, y) => 
                    x + "," + TableTools.removeInvalidChars(y)));
                
                StringBuilder result = new StringBuilder("insert into ")
                    .Append(TableTools.removeInvalidChars(tb))
                    .Append("(")
                    .Append(wrkBldr)
                    .Append(") values(@")
                    .Append(wrkBldr.Replace(",",",@"))
                    .Append(");");

                return result.ToString();
            }

            return "";
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