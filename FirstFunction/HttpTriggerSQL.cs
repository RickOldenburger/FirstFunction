using System;
using System.Runtime.InteropServices;

namespace SQLConnection
{
    public class SqlData
    {
        public string connection { get; set; }

        public void SQLData(string conn = "")
        {
            if (conn == string.Empty)
                connection = connect();
            else
                connection = conn;
        }

        public static string connect()
        {
            string connStr = Environment.GetEnvironmentVariable("connection_string", EnvironmentVariableTarget.Process);
            string password = Environment.GetEnvironmentVariable("CustomCONNSTR_password", EnvironmentVariableTarget.Process);
            string user = Environment.GetEnvironmentVariable("CustomCONNSTR_user", EnvironmentVariableTarget.Process);

            return connStr.Replace("{user}", user).Replace("{password}", password);
        }
    }
}