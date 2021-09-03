namespace AzureEventHubOverKafkaLab
{
    class EventHubConfig
    {
        public string Endpoint { get; set; }
        public string ConsumerGroup { get; set; }
        public string Topic { get; set; }
    }
}