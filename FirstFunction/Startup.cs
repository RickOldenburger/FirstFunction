// Startup.cs
//using Microsoft.Azure.ServiceBus.Core; //Microsoft.Azure.ServiceBus v5.2.0

// https://github.com/serilog-contrib/serilog-sinks-splunk/blob/dev/sample/Sample/Program.cs Splunk sample

using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using Serilog;
using SerilogNamespace;
//using Microsoft.Extensions.Logging; //This does not work right now, needed for ClearProviders()

[assembly: FunctionsStartup(typeof(My.Function.Startup))]
namespace My.Function
{
    public class Startup : FunctionsStartup
    {
        const string SPLUNK_ENDPOINT = "https://splunk-hec.machinedata.illinois.edu:8088"; //  Your splunk url 
        const string SPLUNK_HEC_TOKEN = "ca1a4a25-806a-4f5b-bf83-42b4732d2edb"; // Your HEC token.
        const string SPLUNK_LOG_LEVEL = "WARNING";
        const string CONSOLE_TEMPLATE = "";
        const string SPLUNK_URI_PATH = "services/collector/event";
        const string SPLUNK_SOURCE = "HttpTrigger1";
        const string SPLUNK_SOURCE_TYPE = "TestDataBase";
        const string SPLUNK_INDEX = "apps-sandbox-dev_techsvc-sdg";

        public override void Configure(IFunctionsHostBuilder builder)
        {

            IConfigurationRoot configuration = new ConfigurationBuilder()
                .SetBasePath(Environment.CurrentDirectory)
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .Build();
            
            builder.Services.AddLogging(lb => lb
                //.ClearProviders() // Generates error: Value cannot be null. (Parameter 'provider')
                .AddSerilog(Seriloger.CreateSeriloger(SPLUNK_ENDPOINT, SPLUNK_HEC_TOKEN, 
                    SPLUNK_LOG_LEVEL, CONSOLE_TEMPLATE, SPLUNK_URI_PATH, SPLUNK_SOURCE,
                    SPLUNK_SOURCE_TYPE, SPLUNK_INDEX), true));
        }
    }
}