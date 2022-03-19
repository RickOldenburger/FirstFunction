// Startup.cs
//using Microsoft.Azure.ServiceBus.Core; //Microsoft.Azure.ServiceBus v5.2.0

// https://github.com/serilog-contrib/serilog-sinks-splunk/blob/dev/sample/Sample/Program.cs Splunk sample

using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using Serilog;
using Serilog.Events;
using Microsoft.Extensions.Logging; //This does not work right now, needed for ClearProviders()

[assembly: FunctionsStartup(typeof(My.Function.Startup))]
namespace My.Function
{
    public class Startup : FunctionsStartup
    {
        const string SPLUNK_FULL_ENDPOINT = "https://splunk-hec.machinedata.illinois.edu:8088/services/collector/"; // Full splunk url 
        const string SPLUNK_ENDPOINT = "https://splunk-hec.machinedata.illinois.edu:8088"; //  Your splunk url 
        const string SPLUNK_HEC_TOKEN = "ca1a4a25-806a-4f5b-bf83-42b4732d2edb"; // Your HEC token.
        const string SPLUNK_INDEX = "apps-sandbox-dev_techsvc-sdg";
        public override void Configure(IFunctionsHostBuilder builder)
        {
            IConfigurationRoot configuration = new ConfigurationBuilder()
                .SetBasePath(Environment.CurrentDirectory)
                .AddJsonFile("appsettings.json")
                .Build();

            Serilog.Core.Logger logger = new LoggerConfiguration()
                .WriteTo.Console(outputTemplate: "[{Timestamp:yyyy-MM-dd HH:mm:ss.fff}] [{Level}] {RequestId}-{SourceContext}: {Message}{NewLine}{Exception}")
                .WriteTo.EventCollector(splunkHost: SPLUNK_ENDPOINT
                    , eventCollectorToken: SPLUNK_HEC_TOKEN
                    , uriPath: "services/collector/event"
                    , source: "HttpTrigger1"
                    , sourceType: "TESTSERVER"
                    , host: Environment.MachineName
                    , index: SPLUNK_INDEX)
                //.ReadFrom.Configuration(configuration)
                .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
                .MinimumLevel.Override("System", LogEventLevel.Warning)
                .MinimumLevel.Warning()
                .Enrich.FromLogContext()
                .Enrich.WithMachineName()
                .Enrich.WithProcessId()
                .Enrich.WithThreadId()
                .CreateLogger();
            
            builder.Services.AddLogging(lb => lb
                //.ClearProviders()
                .AddSerilog(logger, true));
           
        }
    }

    // public interface IService
    // {
    // }
    // public class Service : IService
    // {
    //     private ILogger<Service> _logger;
    //     public Service(ILogger<Service> logger)
    //     {
    //         _logger = logger;
    //     }
    // }
}