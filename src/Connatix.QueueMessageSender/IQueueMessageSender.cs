using System;

namespace Connatix.QueueMessageSender
{
    /// <summary>
    /// This interface is the abstraction of the QueueMessageSender class 
    /// </summary>
    public interface IQueueMessageSender : IDisposable
    {
        /// <summary>
        ///     This field keeps the count of the enqueued messages
        /// </summary>      
        int EnqueuedMessageCount { get; }

        /// <summary>
        ///     This field keeps the count of all messages that were successfully sent
        /// </summary>
        int SentMessageCount { get; }

        /// <summary>
        ///     This field keeps the count of all messages that failed to be sent
        /// </summary>
        int FailedMessageCount { get; }

        /// <summary>
        ///     Enqueues a message to be delivered to a named writer stream
        /// </summary>
        /// <param name="message">The payload.</param>
        /// <param name="channelName">Name of the channel.</param>
        void Enqueue(object message, string channelName);

        /// <summary>
        ///     Waits for all the messages to be sent by the message handler
        /// </summary>
        void WaitForIdle();

        /// <summary>
        /// Each new channel has a default weight of 1. If you need to increase or decrease the threads allocated
        /// to a specific channel, use this function. The higher the number the more resources are dedicated to
        /// a channel. Bear in mind that the MaxThreadCount setting (default is 2) is still considered. 
        /// </summary>
        /// <param name="weight"></param>
        /// <param name="channelName"></param>
        void SetChannelWeight(float weight, string channelName);
    }
}

