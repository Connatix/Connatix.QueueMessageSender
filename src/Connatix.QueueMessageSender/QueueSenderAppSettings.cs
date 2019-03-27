namespace Connatix.QueueMessageSender
{
    /// <summary>
    ///     The configuration class for the QueueMessageSender.  
    /// </summary>
    public class QueueMessageSenderAppSettings
    {
        /// <summary>
        /// If enabled then the QueueMessageSender records usage statistics
        /// </summary>
        public bool StatisticsEnabled { get; set; }
        /// <summary>
        /// The max number of threads to be used by this component
        /// </summary>
        public int MaxThreadCount {get;set;}
        /// <summary>
        /// Max number of messages to be handled by a single thread. New messages will be directed to new threads.
        /// </summary>
        public int RequestsPerThread {get;set;}
    }

}