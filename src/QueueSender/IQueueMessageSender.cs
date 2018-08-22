using System;

namespace QueueSender
{
    public interface IQueueMessageSender : IDisposable
    {
        int EnqueuedMessageCount { get; }
        int SentMessageCount { get; }
        int FailedMessageCount { get; }

        void Enqueue(object payload, string shardName);

        void WaitForIdle();
        void SetShardWeight(float weight, string shardName);
    }
}

