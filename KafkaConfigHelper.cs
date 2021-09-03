using System.Text.RegularExpressions;
using Confluent.Kafka;

namespace AzureEventHubOverKafkaLab
{
    static class KafkaConfigHelper
    {
        private static Regex fqdnRegex = new Regex(@"([\w,-]*.servicebus.windows.net)", RegexOptions.Compiled);

        public static ClientConfig
            CreateClientConfig(string connStr) =>
            new ClientConfig()
            {
                BootstrapServers = $"{fqdnRegex.Match(connStr).Value}:9093",
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "$ConnectionString",
                SaslPassword = connStr,
                //SslCaLocation = cacertlocation
            };

        public static ProducerConfig ToProducerConfig(this ClientConfig clientConfig) => new ProducerConfig
        {
            BootstrapServers = clientConfig.BootstrapServers,
            SecurityProtocol = clientConfig.SecurityProtocol,
            SaslMechanism = clientConfig.SaslMechanism,
            SaslUsername = clientConfig.SaslUsername,
            SaslPassword = clientConfig.SaslPassword,
            //SslCaLocation = clientConfig.SslCaLocation
        };

        public static ConsumerConfig ToConsumerConfig(this ClientConfig clientConfig, string consumerGroup) =>
            new ConsumerConfig
            {
                BootstrapServers = clientConfig.BootstrapServers,
                SecurityProtocol = clientConfig.SecurityProtocol,
                SaslMechanism = clientConfig.SaslMechanism,
                SaslUsername = clientConfig.SaslUsername,
                SaslPassword = clientConfig.SaslPassword,
                //SslCaLocation = clientConfig.SslCaLocation,
                GroupId = consumerGroup,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                BrokerVersionFallback = "1.0.0",
                SocketTimeoutMs = 60000,
                SessionTimeoutMs = 30000,
            };
    }
}