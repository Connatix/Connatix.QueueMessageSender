using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Connatix.QueueMessageSender
{
    /// <summary>
    ///     This class provides a thread-safe mechanism to send messages to a external stream writer 
    ///     such as AWS Kinesis or RabbitMQ
    /// </summary>
    /// <seealso cref="System.IDisposable" />
    public class QueueMessageSender : IQueueMessageSender
    {
        private const int MIN_THREADS = 1;
        private Func<List<Message>, string, Task<List<Message>>> m_batchAction;
        private readonly CancellationTokenSource m_cancellationTokenSource = new CancellationTokenSource();
        private readonly AutoResetEvent m_coreInUseFreedFlag;
        private readonly Dictionary<Guid, int> m_coresInUse = new Dictionary<Guid, int>();
        private readonly object m_coresInUseSync = new object();
        private readonly object m_enqueueSync = new object();
        private readonly List<WaitHandle> m_flags = new List<WaitHandle>();
        private readonly object m_flagsSync = new object();
        private readonly ManualResetEvent m_idle;        
        private readonly TimeSpan m_sendLoopCycleDuration = TimeSpan.FromMilliseconds(20);

        private readonly ConcurrentDictionary<string, Channel> m_channels
            = new ConcurrentDictionary<string, Channel>();

        private readonly Task m_taskRunner;
        private int m_enqueuedMessageCount;
        private int m_failedMessageCount;
        private int m_sentMessageCount;
        private readonly QueueMessageSenderAppSettings m_settings;
        private readonly ILogger m_logger;

        /// <summary>
        ///     Initializes a new instance of the <see cref="QueueMessageSender" /> class.
        /// </summary>        
        /// <param name="settings"></param>
        /// <param name="logger"></param>        
        public QueueMessageSender(QueueMessageSenderAppSettings settings, ILogger logger)
        {            
            m_flags.Add(m_cancellationTokenSource.Token.WaitHandle);
            m_idle = new ManualResetEvent(true);
            m_coreInUseFreedFlag = new AutoResetEvent(false);
            m_taskRunner = Task.Run(() => { TaskRunner(); }, m_cancellationTokenSource.Token);
            SentMessageCount = 0;
            m_settings = settings;
            m_logger = logger;
        }
        
        /// <summary>
        ///     This function assigns a message handler for all the enqueued messages. For further details,
        ///     see <see cref="GlobalMessageHandler" /> 
        /// </summary>
        /// <param name="msgHandler"></param>
        public void AddMessageHandler(GlobalMessageHandler msgHandler)
        {
            m_batchAction = async (list, s) =>
            {
                try
                {
                    await msgHandler.Action(list);
                    return new List<Message>();
                }
                catch (Exception)
                {
                    try
                    {
                        await msgHandler.HandleError(list);
                    }
                    catch
                    {
                        // ignored
                    }

                    return list;
                }                
            };
        }

        /// <summary>
        ///     This function assigns a message handler for all the enqueued messages. For further details,
        ///     see <see cref="ChannelSpecificMessageHandler" />
        /// </summary>
        /// <param name="msgHandler"></param>
        public void AddMessageHandler(ChannelSpecificMessageHandler msgHandler)
        {
            m_batchAction = async (list, s) =>
            {
                try
                {
                    await msgHandler.Action(list, s);
                    return new List<Message>();
                }
                catch (Exception)
                {
                    try
                    {
                       await msgHandler.HandleError(list, s);
                    }
                    catch
                    {
                        // ignored
                    }
                    return list;
                }
            };
        }

        /// <summary>
        ///     This function assigns a message handler for all the enqueued messages. For further details,
        ///     see <see cref="CustomMessageHandler" /> 
        /// </summary>
        /// <param name="msgHandler"></param>
        public void AddMessageHandler(CustomMessageHandler msgHandler)
        {
            m_batchAction = async (list, s) =>
            {
                List<Message> failedMessages =new List<Message>();
                try
                {
                    failedMessages.AddRange(await msgHandler.Func(list, s));
                    return failedMessages;
                }
                catch (Exception)
                {
                    try
                    {                        
                        await msgHandler.HandleError(list, s);
                    }
                    catch
                    {
                        // ignored
                    }
                    return list;
                }
            };
        }
       
        /// <summary>
        ///     This field keeps the count of all messages that were successfully sent
        /// </summary>
        /// <remarks>
        ///     If the configuration flag StatisticsEnabled is set to false then SentMessageCount value is 0
        /// </remarks>
        public int SentMessageCount
        {
            get => m_sentMessageCount;
            private set => m_sentMessageCount = value;
        }

        /// <summary>
        ///     This field keeps the count of the enqueued messages
        /// </summary>
        /// <remarks>
        ///     If the configuration flag StatisticsEnabled is set to false then SentMessageCount value is 0
        /// </remarks>
        public int EnqueuedMessageCount => m_enqueuedMessageCount;

        /// <summary>
        ///     This field keeps the count of all messages that failed to be sent
        /// </summary>
        /// <remarks>
        ///     If the configuration flag StatisticsEnabled is set to false then SentMessageCount value is 0
        /// </remarks>
        public int FailedMessageCount => m_failedMessageCount;

        /// <summary>
        ///     Enqueues a message to be delivered to a named writer stream
        /// </summary>
        /// <param name="message">The payload.</param>
        /// <param name="channelName">Name of the channel.</param>
        public void Enqueue(object message, string channelName)
        {
            if (null == message)
                throw new ArgumentException("The argument cannot be null or empty", nameof(message));
            
            if(string.IsNullOrEmpty(channelName))
                throw new ArgumentException("The argument cannot be null or empty", nameof(channelName));
            
            m_idle.Reset();
            if (!m_channels.TryGetValue(channelName, out var channel))
                lock (m_enqueueSync)
                {
                    channel = new Channel(channelName, 1);
                    var newOrExisting = m_channels.GetOrAdd(channelName, channel);
                    if (newOrExisting.GetHashCode() == channel.GetHashCode())
                        lock (m_flagsSync)
                        {
                            m_flags.Add(channel.HasMessages);
                        }

                    newOrExisting.Enqueue(message);
                }
            else
                channel.Enqueue(message);
        }

        /// <summary>
        /// Each new channel has a default weight of 1. If you need to increase or decrease the threads allocated
        /// to a specific channel, use this function. The higher the number the more resources are dedicated to
        /// a channel. Bear in mind that the MaxThreadCount setting (default is 2) is still considered. 
        /// </summary>
        /// <param name="weight"></param>
        /// <param name="channelName"></param>
        public void SetChannelWeight(float weight, string channelName)
        {
            if (!m_channels.TryGetValue(channelName, out var channel))
                lock (m_enqueueSync)
                {
                    channel = new Channel(channelName, weight);
                    var newOrExistingChannel = m_channels.GetOrAdd(channelName, channel);
                    if (newOrExistingChannel.GetHashCode() != channel.GetHashCode()) 
                        return;
                    
                    lock (m_flagsSync)
                    {
                        m_flags.Add(channel.HasMessages);
                    }
                }
            else
                channel.Weight = weight;
        }

        /// <summary>
        ///     Waits for all the messages to be sent by the message handler
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
            foreach (var channel in m_channels.Values)
                channel.Dispose();
            m_channels.Clear();
            m_cancellationTokenSource.Dispose();
        }

        /// <summary>
        ///     The long running task that executes the Channel action callback  (and error handler when required)
        ///     whenever the messages are available
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
                    var totalMessages = m_channels.Values.Sum(queue => queue.GetSize());
                    if (totalMessages == 0)
                    {
                        lock (m_coresInUseSync)
                        {
                            if (m_coresInUse.Count == 0)
                                m_idle.Set();
                        }

                        continue;
                    }

                    //get all the channels that have at least one batch to send
                    var channels = m_channels.Values.Where(x => x.GetSize() > 0).ToList();
                    //calculate the sum of weights for the channels that have at least one batch to send                    
                    var totalWeight = channels.Sum(x => x.Weight);
                    foreach (var channel in m_channels.Values)
                    {
                        var options = GetParallelDegree(totalMessages, channel, totalWeight);
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
                                Parallel.ForEach(channel.TakeBatches(), options, batch =>
                                {
                                    var failedMessages =  ExecuteBatchAction(batch.Item2, batch.Item1.Name);
                                    if (m_settings.StatisticsEnabled)                                     
                                        Interlocked.Add(ref m_enqueuedMessageCount, batch.Item2.Count);
                                    if (failedMessages.Count > 0)
                                    {                                        
                                        if (m_settings.StatisticsEnabled)
                                            Interlocked.Add(ref m_failedMessageCount, failedMessages.Count);                                        
                                        batch.Item1.AddFailedRequests(failedMessages);
                                    }
                                    
                                    if (m_settings.StatisticsEnabled)
                                        Interlocked.Add(ref m_sentMessageCount,
                                            batch.Item2.Count - failedMessages.Count);
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
                    m_logger.LogCritical(ex, "An exception occurred into QueueMessageSender.TaskRunner");
                }
        }

        /// <summary>
        ///     Executes the batch action and in case of exceptions, the input messages are considered faulty
        ///     so they are all returned as failed messages
        /// </summary>
        /// <param name="batch">list of messages to be processed</param>
        /// <param name="channelName">the name of the channel</param>
        /// <returns>The messages that failed to be delivered</returns>
        private List<Message> ExecuteBatchAction(List<Message> batch, string channelName)
        {
            if(null == m_batchAction)
                return new List<Message>();
            
            var failedMessages = new List<Message>();
            try
            {
                var task = m_batchAction(batch, channelName);
                task.Wait();
                failedMessages = task.Result;
            }
            catch (Exception e)
            {
                foreach (var msg in batch)
                {
                    var messageToAddBack = new Message
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
        ///     Decides the number of threads to be used. The following criteria are considered:
        ///         - the number of messages waiting to be delivered,
        ///         - weight of the channel
        ///         - count of other channels that have batches to send.
        /// </summary>
        /// <remarks>
        ///     If there are no cores left, the function waits for the completion of other tasks.
        /// </remarks>
        /// <param name="messageCount">The message count.</param>
        /// <param name="channel"></param>
        /// <param name="channelsTotalWeight"></param>
        /// <returns></returns>
        private ParallelOptions GetParallelDegree(int messageCount, Channel channel, float channelsTotalWeight)
        {            
            var maxThreadCount = m_settings.MaxThreadCount;
            //calculate the number of needed cores based on the weight of the Channel
            var requiredThreadCount = Math.Ceiling(channel.Weight / channelsTotalWeight) * maxThreadCount;         
            var requestsPerThread = m_settings.RequestsPerThread;

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

        /// <summary>
        /// Returns the number of threads that are available for channels.
        /// </summary>
        /// <param name="requestedThreadCount"></param>
        /// <returns></returns>
        private int GetRemainingThreadCount(int requestedThreadCount)
        {
            int remainingThreadCount;
            var maxThreadCount = m_settings.MaxThreadCount;
            lock (m_coresInUseSync)
            {
                var totalCoresInUse = m_coresInUse.Sum(x => x.Value);

                remainingThreadCount = Math.Min(maxThreadCount - totalCoresInUse,
                    requestedThreadCount);
            }

            return remainingThreadCount;
        }


        /// <summary>
        ///     Gets the queue size all channels
        /// </summary>
        /// <returns></returns>
        public int GetPendingMessageCount()
        {
            return m_channels.Values.Sum(queue => queue.GetSize());
        }
    }

    /// <summary>
    ///     Provides batch mode and thread-safe access to messages about to be delivered to a channel
    /// </summary>
    /// <seealso cref="System.IDisposable" />
    internal class Channel : IDisposable
    {
        private const int MAX_BATCH_SIZE = 500;
        private readonly List<Message> m_messages;
        private readonly object m_sync;

        public Channel(string name, float weight)
        {
            m_sync = new object();
            m_messages = new List<Message>();
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
                m_messages.Add(new Message
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
        public void AddFailedRequests(List<Message> failedMessages)
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
        public List<Tuple<Channel, List<Message>>> TakeBatches()
        {
            List<List<Message>> result;
            lock (m_sync)
            {
                result = Split(m_messages, MAX_BATCH_SIZE);
                m_messages.Clear();
                HasMessages.Reset();
            }

            return result
                .Where(x => x.Count > 0)
                .Select(x => new Tuple<Channel, List<Message>>(this, x)).ToList();
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

        /// <summary>
        /// Splits a list of messages into multiple list of lists. each sub-list has the length = "size",
        /// except for the last sub-list, which can be smaller
        /// </summary>
        /// <param name="collection"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        private static List<List<Message>> Split(List<Message> collection, int size)
        {   
            if (size == 0)
                throw new ArgumentException();
            
            var chunks = new List<List<Message>>();
            var chunkCount = collection.Count / size;

            if (collection.Count % size > 0)
                chunkCount++;

            for (var i = 0; i < chunkCount; i++)
                chunks.Add(collection.Skip(i * size).Take(size).ToList());

            return chunks;
        }
    }    

    /// <summary>
    /// This class encapsulates an enqueued message. It also tracks the retry count and the last error
    /// of the message handler action.
    /// </summary>
    public class Message
    {
        public int RetryCount { get; set; }
        public object Payload { get; set; }
        public string LastErrorMessage { get; set; }
    }
}