using System;

namespace QueueSender
{
    public interface IQueueMessageSender : IDisposable
    {
        int EnqueuedMessageCount { get; }
        int SentMessageCount { get; }
        int FailedMessageCount { get; }

        void Enqueue(object message, string channelName);

        void WaitForIdle();
        void SetChannelWeight(float weight, string channelName);
    }
}

