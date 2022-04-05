using System;

namespace JsonTools
{
    public class SQLConnection
    {

        private string _connection;
        public string connection { get { return _connection; } }

        public SQLConnection(string conn = "")
        {
            if (conn == "")
                _connection = connect();
            else
                _connection = conn;
        }

        public virtual string connect()
        {
            string connStr = Environment.GetEnvironmentVariable("connection_string", EnvironmentVariableTarget.Process);
            string password = Environment.GetEnvironmentVariable("CustomCONNSTR_password", EnvironmentVariableTarget.Process);
            string user = Environment.GetEnvironmentVariable("CustomCONNSTR_user", EnvironmentVariableTarget.Process);

            return connStr.Replace("{user}", user).Replace("{password}", password);
        }
    }
}