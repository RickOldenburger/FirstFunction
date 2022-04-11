// Startup.cs
//using Microsoft.Azure.ServiceBus.Core; //Microsoft.Azure.ServiceBus v5.2.0

// https://github.com/serilog-contrib/serilog-sinks-splunk/blob/dev/sample/Sample/Program.cs Splunk sample

using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using Serilog;
using CustomLogger;
//using Microsoft.Extensions.Logging; //This does not work right now, needed for ClearProviders()

[assembly: FunctionsStartup(typeof(My.Function.Startup))]
namespace My.Function
{
    public class Startup : FunctionsStartup
    {
        public override void Configure(IFunctionsHostBuilder builder)
        {
          try {
            // sleep
            // System.Threading.Thread.Sleep(4000);
            Console.WriteLine("[DEBUG] [Startup.Configure] Running startup.");

            // Get the original configuration provider from the Azure Function
            var configProvider = builder.Services.BuildServiceProvider().GetService<IConfiguration>();
            IConfigurationRoot config = new ConfigurationBuilder()
                .SetBasePath(Environment.CurrentDirectory)
                .AddConfiguration(configProvider) // Add the original function configuration 
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .Build();

            builder.Services.AddSingleton(config);
            builder.Services.AddLogging(logBuilder => logBuilder
              .AddSerilog(Serilogger.Create(config), true)
            );
            // todo clear providers...?

           } catch (Exception e) {
            // Manually log error to console since logger code failed to initialize
            Console.ForegroundColor = ConsoleColor.Red;
            Console.Error.WriteLine($"[DEBUG] [Startup.Configure] Error: {e.Message}");
            Console.Error.WriteLine(e.StackTrace);
            Console.ResetColor();
            // Propogate
            throw e;
          }
    
        }
    }
}