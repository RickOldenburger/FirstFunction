using System;

namespace JsonTools
{
    public class SQLConnection
    {
        private string _envConnection;
        public string envConnection { 
            get { return _envConnection;}
            set { _envConnection = value; }}

        private string _envUser;
        public string envUser {
            get { return _envUser; }
            set { _envUser = value; }}

        private string _envPassword;
        public string envPassword {
            get { return _envPassword; }
            set { _envPassword = value; }}

        private string _connection;
        public string connection { 
            get { return _connection; } 
            set { _connection = value; }}

        public SQLConnection(string envConnection = "connection_string",
            string envUser = "CustomCONNSTR_user", string envPassword = "CustomCONNSTR_password")
        {
            _connection = connect(envConnection, envUser, envPassword);
        }


        public virtual string connect(string envConnection = null, 
            string envUser = null, string envPassword = null)
        {
            envConnection = (envConnection ?? _envConnection ?? "");
            envUser = (envUser ?? _envUser ?? "");
            envPassword = (envPassword ?? _envPassword ?? "");

            string connStr = Environment.GetEnvironmentVariable(envConnection, EnvironmentVariableTarget.Process);
            string user = Environment.GetEnvironmentVariable(envUser, EnvironmentVariableTarget.Process) ?? "";
            string password = Environment.GetEnvironmentVariable(envPassword, EnvironmentVariableTarget.Process) ?? "";

            return connStr.Replace("{user}", user).Replace("{password}", password);
        }

        public void SetConnect(string envConnection = null, 
            string envUser = null, string envPassword = null)
        {
            connection = connect(envConnection, envUser, envPassword);
        }
    }
}