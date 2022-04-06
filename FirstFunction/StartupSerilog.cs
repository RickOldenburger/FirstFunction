using System;
using Serilog;
using Serilog.Events;
using Serilog.Core;

namespace CustomLogger
{
    public static class Serilogger
    {
        const string _template = "[{Timestamp:yyyy-MM-dd HH:mm:ss.fff}] [{Level}] {RequestId}-{SourceContext}: {Message}{NewLine}{Exception}";

        public static Logger Create(string endpoint, string hecToken, string logLevel = "INFORMATION",
            string template = _template, string uriPath = "services/collector", string source = "", 
            string sourceType = "", string index = "")
        {
            LoggingLevelSwitch _levelSwitch = new LoggingLevelSwitch(getLogLevelByName(logLevel));
            if (template == "")
                template = _template;

            return (new LoggerConfiguration()
                .WriteTo.Console(outputTemplate: template)
                .WriteTo.EventCollector(splunkHost: endpoint
                    , eventCollectorToken: hecToken
                    , uriPath: uriPath
                    , source: source
                    , sourceType: sourceType
                    , host: Environment.MachineName
                    , index: index)
                //.ReadFrom.Configuration(configuration) Broken in .net5 and .net6
                .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
                .MinimumLevel.Override("System", LogEventLevel.Warning)
                .MinimumLevel.ControlledBy(_levelSwitch)
                .Enrich.FromLogContext()
                .Enrich.WithMachineName()
                .Enrich.WithProcessId()
                .Enrich.WithThreadId()
                .CreateLogger());
        }

        public static LogEventLevel getLogLevelByName(string eventLevel)
        {
            switch (eventLevel.ToUpper())
            {
                case "NONE":
                case "CRITICAL":
                    return LogEventLevel.Fatal;
                case "ERROR":
                    return LogEventLevel.Error;
                case "WARNING":
                    return LogEventLevel.Warning;
                case "INFORMATION":
                    return LogEventLevel.Information;
                case "DEBUG":
                    return LogEventLevel.Debug;
                default:
                    return LogEventLevel.Verbose;
            }
        }
    }
}