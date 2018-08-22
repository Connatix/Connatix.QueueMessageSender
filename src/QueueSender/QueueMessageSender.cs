using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NLog;

namespace QueueSender
{
    /// <summary>
    ///     This class provides a thread-safe mechanism to send messages to an external stream provider, such as Kinesis or
    ///     RabbitMQ
    /// </summary>
    /// <seealso cref="System.IDisposable" />
    public class QueueMessageSender : IQueueMessageSender
    {
        private const int MIN_THREADS = 1;
        private readonly Func<List<QueueMessage>, string, Task<List<QueueMessage>>> m_batchAction;
        private readonly CancellationTokenSource m_cancellationTokenSource = new CancellationTokenSource();
        private readonly AutoResetEvent m_coreInUseFreedFlag;
        private readonly Dictionary<Guid, int> m_coresInUse = new Dictionary<Guid, int>();
        private readonly object m_coresInUseSync = new object();
        private readonly object m_enqueueSync = new object();
        private readonly List<WaitHandle> m_flags = new List<WaitHandle>();
        private readonly object m_flagsSync = new object();
        private readonly ManualResetEvent m_idle;
        private readonly ILogger m_logger = LogManager.GetCurrentClassLogger();
        private readonly TimeSpan m_sendLoopCycleDuration = TimeSpan.FromMilliseconds(20);

        private readonly ConcurrentDictionary<string, QueueShard> m_shardQueues
            = new ConcurrentDictionary<string, QueueShard>();

        private readonly Task m_taskRunner;
        private int m_enqueuedMessageCount;
        private int m_failedMessageCount;
        private int m_sentMessageCount;
        private QueueSenderAppSettings _settings;

        /// <summary>
        ///     Initializes a new instance of the <see cref="QueueMessageSender" /> class.
        /// </summary>
        /// <param name="batchAction">
        ///     A function that has arguments: 1) a list of messages to be sent, 2) the name of the shard.
        ///     The function must return a list of messages that could not be sent
        /// </param>        
        public QueueMessageSender(
            Func<List<QueueMessage>, string, Task<List<QueueMessage>>> batchAction, 
            QueueSenderAppSettings settings)
        {
            m_batchAction = batchAction;
            m_flags.Add(m_cancellationTokenSource.Token.WaitHandle);
            m_idle = new ManualResetEvent(true);
            m_coreInUseFreedFlag = new AutoResetEvent(false);
            m_taskRunner = Task.Run(() => { TaskRunner(); }, m_cancellationTokenSource.Token);
            SentMessageCount = 0;
            _settings = settings;
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="QueueMessageSender" /> class.
        /// </summary>
        /// <param name="batchAction"></param>
        public QueueMessageSender(
            Func<List<QueueMessage>, string, List<QueueMessage>> batchAction, 
            QueueSenderAppSettings settings)
            : this((messages, shardName) => { return Task.Run(() => batchAction(messages, shardName));}, 
            settings)
        {
        }
       
        public int SentMessageCount
        {
            get => m_sentMessageCount;
            private set => m_sentMessageCount = value;
        }

        public int EnqueuedMessageCount => m_enqueuedMessageCount;

        public int FailedMessageCount => m_failedMessageCount;

        /// <summary>
        ///     Enqueues a payload to be delivered to a stream, such as kinesis
        /// </summary>
        /// <param name="payload">The payload.</param>
        /// <param name="shardName">Name of the shard.</param>
        public void Enqueue(object payload, string shardName)
        {
            m_idle.Reset();
            if (!m_shardQueues.TryGetValue(shardName, out var shardQueue))
                lock (m_enqueueSync)
                {
                    shardQueue = new QueueShard(shardName, 1);
                    var newOrExistingQueueShard = m_shardQueues.GetOrAdd(shardName, shardQueue);
                    if (newOrExistingQueueShard.GetHashCode() == shardQueue.GetHashCode())
                        lock (m_flagsSync)
                        {
                            m_flags.Add(shardQueue.HasMessages);
                        }

                    newOrExistingQueueShard.Enqueue(payload);
                }
            else
                shardQueue.Enqueue(payload);
        }

        public void SetShardWeight(float weight, string shardName)
        {
            if (!m_shardQueues.TryGetValue(shardName, out var shardQueue))
                lock (m_enqueueSync)
                {
                    shardQueue = new QueueShard(shardName, weight);
                    var newOrExistingQueueShard = m_shardQueues.GetOrAdd(shardName, shardQueue);
                    if (newOrExistingQueueShard.GetHashCode() == shardQueue.GetHashCode())
                        lock (m_flagsSync)
                        {
                            m_flags.Add(shardQueue.HasMessages);
                        }
                }
            else
                shardQueue.Weight = weight;
        }

        /// <summary>
        ///     Waits for completing all the messages to be processed
        /// </summary>
        public void WaitForIdle()
        {
            try
            {
                WaitHandle.WaitAny(new[] {m_cancellationTokenSource.Token.WaitHandle, m_idle});
            }
            catch (Exception)
            {
                //very likely someone called dispose and then checked IsIdle
            }
        }

        /// <summary>
        ///     Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            m_cancellationTokenSource.Cancel();
            m_taskRunner.Wait();
            m_taskRunner.Dispose();
            
            lock (m_flagsSync)
            {
                m_flags.Clear();
            }

            m_idle.Dispose();
            m_coreInUseFreedFlag.Dispose();
            foreach (var shardQueue in m_shardQueues.Values)
                shardQueue.Dispose();
            m_shardQueues.Clear();
            m_cancellationTokenSource.Dispose();
        }

        /// <summary>
        ///     The long running task that executes the QueueShard callback whenever the messages are available
        /// </summary>
        private void TaskRunner()
        {
            while (true)
                try
                {
                    var waitResult = WaitHandle.WaitAny(m_flags.ToArray(), TimeSpan.FromMilliseconds(200));
                    //this means that Dispose has been called
                    if (waitResult == 0)
                        return;

                    //count all the messages so that the thread affinity is chosen
                    var totalMessages = m_shardQueues.Values.Sum(queue => queue.GetSize());
                    if (totalMessages == 0)
                    {
                        lock (m_coresInUseSync)
                        {
                            if (m_coresInUse.Count == 0)
                                m_idle.Set();
                        }

                        continue;
                    }

                    //get all the shards that have at least one batch to send
                    var shards = m_shardQueues.Values.Where(x => x.GetSize() > 0).ToList();
                    //calculate the sum of weights for the shards that have at least one batch to send                    
                    var totalWeight = shards.Sum(x => x.Weight);
                    foreach (var shard in m_shardQueues.Values)
                    {
                        var options = GetParallelDegree(totalMessages, shard, totalWeight);
                        //check if cancellation requested                            
                        if (null == options)
                            return;

                        var taskGuid = Guid.NewGuid();
                        lock (m_coresInUseSync)
                        {
                            m_coresInUse[taskGuid] = options.MaxDegreeOfParallelism;
                        }

                        Task.Factory.StartNew(state =>
                            {
                                Parallel.ForEach(shard.TakeBatches(), options, shardBatch =>
                                {
                                    var failedMessages =  ExecuteBatchAction(shardBatch.Item2, shardBatch.Item1.Name);
                                    if (_settings.StatisticsEnabled)                                     
                                        Interlocked.Add(ref m_enqueuedMessageCount, shardBatch.Item2.Count);
                                    if (failedMessages.Count > 0)
                                    {                                        
                                        if (_settings.StatisticsEnabled)
                                            Interlocked.Add(ref m_failedMessageCount, failedMessages.Count);
                                        //TODO: later, use a max retry, or a message expiration so that it can be moved to a dead-letter queue
                                        shardBatch.Item1.AddFailedRequests(failedMessages);
                                    }
                                    
                                    if (_settings.StatisticsEnabled)
                                        Interlocked.Add(ref m_sentMessageCount,
                                            shardBatch.Item2.Count - failedMessages.Count);
                                });
                            }, taskGuid)
                            .ContinueWith(task =>
                            {
                                lock (m_coresInUseSync)
                                {
                                    m_coresInUse.Remove((Guid) task.AsyncState);
                                    m_coreInUseFreedFlag.Set();
                                }
                            });
                    }

                    if (m_cancellationTokenSource.Token.WaitHandle.WaitOne(m_sendLoopCycleDuration))
                        return;
                }
                catch (ThreadAbortException)
                {
                    //no need to throw because it is automatically raised again at the end of this block
                }
                catch (Exception ex)
                {
                    m_logger.Fatal(ex, "An exception occurred into QueueMessageSender.TaskRunner");
                }
        }

        /// <summary>
        ///     Executes the batch action and in case of exceptions, the input messages are considered faulty
        ///     so they are all returned as failed messages
        /// </summary>
        /// <param name="batch">list of messages to be processed</param>
        /// <param name="shardName">the name of the shard</param>
        /// <returns>The messages that failed to be delivered</returns>
        private List<QueueMessage> ExecuteBatchAction(
            List<QueueMessage> batch, string shardName)
        {
            var failedMessages = new List<QueueMessage>();
            try
            {
                var task = m_batchAction(batch, shardName);
                task.Wait();
                failedMessages = task.Result;
            }
            catch (Exception e)
            {
                foreach (var msg in batch)
                {
                    var messageToAddBack = new QueueMessage
                    {
                        LastErrorMessage = e.Message,
                        Payload = msg.Payload,
                        RetryCount = msg.RetryCount + 1
                    };
                    failedMessages.Add(messageToAddBack);
                }
            }

            return failedMessages;
        }

        /// <summary>
        ///     Decides the number of threads to be used, based on the number of messages waiting to be delivered, weight of the
        ///     queueShard and other shards that have batches to send.
        /// </summary>
        /// <remarks>
        ///     If there are no cores left, the function waits for the completion of other tasks.
        /// </remarks>
        /// <param name="messageCount">The message count.</param>
        /// <param name="queueShard"></param>
        /// <param name="queueShardsTotalWeight"></param>
        /// <returns></returns>
        private ParallelOptions GetParallelDegree(int messageCount, QueueShard queueShard, float queueShardsTotalWeight)
        {            
            var maxThreadCount = _settings.MaxThreadCount;
            //calculate the number of needed cores based on the weight of the QueueShard
            var requiredThreadCount = Math.Ceiling(queueShard.Weight / queueShardsTotalWeight) * maxThreadCount;         
            var requestsPerThread = _settings.RequestsPerThread;

            //requestedThreadCount is a number between MinThreads and the requiredThreadCount number
            var requestedThreadCount = messageCount / requestsPerThread;
            if (requestedThreadCount < MIN_THREADS) requestedThreadCount = MIN_THREADS;
            else if (requestedThreadCount > requiredThreadCount) requestedThreadCount = (int) requiredThreadCount;
            int remainingThreadCount;
            do
            {
                remainingThreadCount = GetRemainingThreadCount(requestedThreadCount);
                if (remainingThreadCount == 0)
                {
                    //this means the associated pool is depleted. need to wait for their completion and to try again
                    var waitResult = WaitHandle.WaitAny(new[]
                        {m_cancellationTokenSource.Token.WaitHandle, m_coreInUseFreedFlag});
                    if (waitResult == 0) return null;
                }
                else
                {
                    //the PFE task can run so the flag being set by another PFE task should not trick the next WaitAny 
                    m_coreInUseFreedFlag.Reset();
                }
            } while (remainingThreadCount == 0);

            return new ParallelOptions
            {
                MaxDegreeOfParallelism = remainingThreadCount
            };
        }

        private int GetRemainingThreadCount(int requestedThreadCount)
        {
            int remainingThreadCount;
            var maxThreadCount = _settings.MaxThreadCount;
            lock (m_coresInUseSync)
            {
                var totalCoresInUse = m_coresInUse.Sum(x => x.Value);

                remainingThreadCount = Math.Min(maxThreadCount - totalCoresInUse,
                    requestedThreadCount);
            }

            return remainingThreadCount;
        }


        /// <summary>
        ///     Gets the queue size all shards
        /// </summary>
        /// <returns></returns>
        public int GetPendingMessageCount()
        {
            return m_shardQueues.Values.Sum(queue => queue.GetSize());
        }
    }

    /// <summary>
    ///     Provides batch mode and thread-safe access to messages about to be delivered to a channel
    /// </summary>
    /// <seealso cref="System.IDisposable" />
    public class QueueShard : IDisposable
    {
        private const int MAX_BATCH_SIZE = 500;
        private readonly List<QueueMessage> m_messages;
        private readonly object m_sync;

        public QueueShard(string name, float weight)
        {
            m_sync = new object();
            m_messages = new List<QueueMessage>();
            HasMessages = new ManualResetEvent(false);
            Name = name;
            Weight = weight;
        }

        public float Weight { get; set; }
        public string Name { get; }
        public ManualResetEvent HasMessages { get; }

        /// <summary>
        ///     Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            lock (m_sync)
            {
                m_messages.Clear();
            }

            HasMessages?.Dispose();
        }

        /// <summary>
        ///     Enqueues the specified payload.
        /// </summary>
        /// <param name="payload">The payload.</param>
        public void Enqueue(object payload)
        {
            lock (m_sync)
            {
                m_messages.Add(new QueueMessage
                {
                    Payload = payload,
                    RetryCount = 0
                });
            }

            HasMessages.Set();
        }

        /// <summary>
        ///     Adds the failed requests to the message list
        /// </summary>
        /// <param name="failedMessages">The failed messages.</param>
        public void AddFailedRequests(List<QueueMessage> failedMessages)
        {
            lock (m_sync)
            {
                m_messages.AddRange(failedMessages);
            }
        }

        /// <summary>
        ///     Removes the messages and returns a batch list, each batch having a max size of 500
        /// </summary>
        /// <returns>Tuple of the object itself and batch list</returns>
        public List<Tuple<QueueShard, List<QueueMessage>>> TakeBatches()
        {
            List<List<QueueMessage>> result;
            lock (m_sync)
            {
                result = Split(m_messages, MAX_BATCH_SIZE);
                m_messages.Clear();
                HasMessages.Reset();
            }

            return result
                .Where(x => x.Count > 0)
                .Select(x => new Tuple<QueueShard, List<QueueMessage>>(this, x)).ToList();
        }

        /// <summary>
        ///     Gets the estimated size of a batch.
        /// </summary>
        /// <returns></returns>
        public int GetSize()
        {
            lock (m_sync)
            {
                return m_messages.Count;
            }
        }

        private static List<List<QueueMessage>> Split<QueueMessage>(List<QueueMessage> collection, int size)
        {   
            if (size == 0)
                throw new ArgumentException();
            
            var chunks = new List<List<QueueMessage>>();
            var chunkCount = collection.Count / size;

            if (collection.Count % size > 0)
                chunkCount++;

            for (var i = 0; i < chunkCount; i++)
                chunks.Add(collection.Skip(i * size).Take(size).ToList());

            return chunks;
        }
    }    

    public class QueueMessage
    {
        public int RetryCount { get; set; }
        public object Payload { get; set; }
        public string LastErrorMessage { get; set; }
    }
}