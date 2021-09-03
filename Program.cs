using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace AzureEventHubOverKafkaLab
{
    class Program
    {
        private static EventHubConfig _eventHubConfig = new();

        static async Task Main(string[] args)
        {
            using var host = CreateHostBuilder(args).Build();
            var factory = new KafkaTasksFactory(_eventHubConfig);
            Console.WriteLine("Starting Producer");
            await factory.Producer();
            Console.WriteLine("Producer Complete");
            Console.WriteLine("Starting Consumer. Ctrl+C to cancel");
            factory.Consumer();
            Console.WriteLine("Consumer Complete");
            await host.RunAsync();
        }

        static IHostBuilder CreateHostBuilder(string[] args)
        {
            return Host.CreateDefaultBuilder(args)
                .ConfigureAppConfiguration((hostingContext, configuration) =>
                {
                    configuration.Sources.Clear();

                    var env = hostingContext.HostingEnvironment;

                    configuration
                        .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                        .AddJsonFile($"appsettings.{env.EnvironmentName}.json", true, true);


                    var configurationRoot = configuration.Build();
                    configurationRoot.GetSection(nameof(EventHubConfig))
                        .Bind(_eventHubConfig);
                });
        }
    }
}