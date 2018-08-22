namespace QueueSender
{
    public class QueueSenderAppSettings
    {
        public bool StatisticsEnabled { get; set; }
        public int MaxThreadCount {get;set;}
        public int RequestsPerThread {get;set;}
    }

}