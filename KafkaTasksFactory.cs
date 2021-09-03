using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace AzureEventHubOverKafkaLab
{
    class KafkaTasksFactory
    {
        private readonly ClientConfig _clientConfig;
        private readonly EventHubConfig _config;
        private string Topic => _config.Topic;

        public KafkaTasksFactory(EventHubConfig config)
        {
            _clientConfig = KafkaConfigHelper.CreateClientConfig(config.Endpoint);
            _config = config;
        }

        public async Task Producer()
        {
            try
            {
                var config = _clientConfig.ToProducerConfig();
                using var producer = new ProducerBuilder<long, string>(config).SetKeySerializer(Serializers.Int64)
                    .SetValueSerializer(Serializers.Utf8).Build();
                Console.WriteLine($"Sending 10 messages to topic: {Topic}");
                for (var x = 0; x < 10; x++)
                {
                    var msg = $"Sample message #{x} sent at {DateTime.Now:yyyy-MM-dd_HH:mm:ss.ffff}";
                    var deliveryReport = await producer.ProduceAsync(Topic,
                        new Message<long, string> {Key = DateTime.UtcNow.Ticks, Value = msg});
                    Console.WriteLine($"Message {x} sent (value: '{msg}')");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Exception Occurred - {e.Message}");
            }
        }

        public void Consumer()
        {
            var config = _clientConfig.ToConsumerConfig(_config.ConsumerGroup);

            using var consumer = new ConsumerBuilder<long, string>(config).SetKeyDeserializer(Deserializers.Int64)
                .SetValueDeserializer(Deserializers.Utf8).Build();
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            consumer.Subscribe(Topic);

            Console.WriteLine($"Consuming messages from topic: {Topic}");

            while (true)
            {
                try
                {
                    var msg = consumer.Consume(cts.Token);
                    Console.WriteLine($"Received: '{msg.Message.Value}'");
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Consume error: {e.Error.Reason}");
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Error: {e.Message}");
                }
            }
        }
    }
}