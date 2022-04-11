using System;
using Serilog;
using Serilog.Events;
using Serilog.Core;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;

namespace CustomLogger
{
    public class SplunkConfig
    {
        public string splunkHost { get; set; } // HostUrl
        public string uriPath { get; set; } = "services/collector";
        public string eventCollectorToken { get; set; }
        public string sourceName { get; set; }
        public string sourceType { get; set; }
        public string index { get; set; }
        public string host { get; set; } = Environment.MachineName;
        public string logLevel { get; set; }

        public SplunkConfig(IConfigurationRoot config) {
            var fieldMappings = new Dictionary<string, string>{
                {"Splunk:HostUrl", "splunkHost"},
                {"Splunk:UriPath", "uriPath"},
                {"Splunk:HecToken", "eventCollectorToken"},
                {"Splunk:Source:Name", "sourceName"},
                {"Splunk:Source:Type", "sourceType"},
                {"Splunk:Index", "index"},
                {"Splunk:Host", "host"},
                {"Splunk:LogLevel", "logLevel"}
            };

            // TODO: Set explicit type
            foreach(var item in config.GetSection("Splunk").AsEnumerable()) {
                try {
                    var key = item.Key;
                    var value = item.Value;

                    var fieldName = fieldMappings[key];
                    // is this necessary
                    if (fieldName.Length > 0 && value.Length > 0) {
                        // Console.WriteLine($"[INFO] Setting field {fieldName} = [{value}]");
                        this.GetType().GetProperty(fieldName).SetValue(this, value);
                    }
                } catch (Exception e) {
                    // todo conditional on specific err code else throw
                    // if (e.
                    if (item.Value?.GetType() == typeof(string)) {
                        Console.WriteLine($"[WARNING] [Serilogger.cs] Could not set value for {item.Key} even though a value is set. Skipping.");
                        return;
                    }
                    // todo possibly ignore this. It logs for values of nested configs which is annoying and unnecessary.
                    Console.WriteLine($"[INFO] [Serilogger.cs] Could not set value for {item.Key}. Skipping.");
                }
            }

        }
    }
    public static class Serilogger
    {
        // Defaults
        const string _template = "[{Timestamp:yyyy-MM-dd HH:mm:ss.fff}] [{Level}] {RequestId}-{SourceContext}: {Message}{NewLine}{Exception}";
        const string _minLogLevel = "INFORMATION";

        public static Logger Create(IConfigurationRoot config)
        {
            Console.WriteLine("[DEBUG] Creating new logger");

            // Get config values
            string minLogLevel = config.GetValue("Serilog:MinimumLevel:Default", _minLogLevel);
            string template = config.GetValue("Serilog:Template", _template);
            IEnumerable<KeyValuePair<string, string>> serilogOverride = config.GetSection("Serilog:MinimumLevel:Override").AsEnumerable();

            // Initialize splunk config
            SplunkConfig splunkConfig = new SplunkConfig(config);

            LoggingLevelSwitch _levelSwitch = new LoggingLevelSwitch(getLogLevelByName(minLogLevel));

            return (new LoggerConfiguration()
                .WriteTo.Console(outputTemplate: template)
                .WriteTo.EventCollector(
                    splunkHost: splunkConfig.splunkHost,
                    eventCollectorToken: splunkConfig.eventCollectorToken,
                    uriPath: splunkConfig.uriPath,
                    source: splunkConfig.sourceName,
                    sourceType: splunkConfig.sourceType,
                    host: splunkConfig.host,
                    index: splunkConfig.index
                )
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