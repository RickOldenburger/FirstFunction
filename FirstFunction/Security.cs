using System;

namespace Security
{
    static class Authorized
    {
        const string update = "Update: ";
        const string read = "Read: ";
        const string readupdate = "ReadUpdate: ";

        private static bool CheckAuthoriation(in string authorization, in string token, in string tokens)
        {
            string wrkStr;
            wrkStr = authorization + token;
            if (tokens.Contains(token))
                return true;
            if (authorization != "Default")
            {
                wrkStr = readupdate + token;
                if (tokens.Contains(token))
                    return true;
            }
            return false;
        }

        public static bool IsAuthorized(string tokenGroup, string token, Level level = Level.Default)
        {
            if (token == "")
                return false;

            string tokens = Environment.GetEnvironmentVariable(tokenGroup, EnvironmentVariableTarget.Process);
            return CheckAuthoriation(level.ToString() ,in token,in tokens);
        }

        public enum Level{
            Default,
            Read,
            Update
        }
    }
}