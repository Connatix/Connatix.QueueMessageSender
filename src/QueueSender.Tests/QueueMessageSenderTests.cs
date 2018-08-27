using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace QueueSender.Tests
{
    public class QueueMessageSenderTests
    {
        private QueueMessageSender m_sut;
        private readonly Mock<IProxy> m_proxy = new Mock<IProxy>();                
        private readonly Mock<ILogger> m_logger = new Mock<ILogger>();
        private readonly QueueSenderAppSettings m_settings = new QueueSenderAppSettings {
            StatisticsEnabled = true,
            MaxThreadCount = 2,
            RequestsPerThread = 100
        };

        [Fact]
        public void LargeNumberOfMessagesAreHandled()
        {
            var writeHasBeenCalled = false;
            const int msgCount = 30000;
            m_proxy.Setup(x => x.WriteAsync(It.IsAny<List<Message>>(), It.IsAny<string>()))
                .ReturnsAsync(new List<Message>());

            m_sut = new QueueMessageSender(m_settings, m_logger.Object);            
            m_sut.AddMessageHandler(new GlobalMessageHandler
            {
                Action = async messages =>
                {
                    writeHasBeenCalled = true;
                    await m_proxy.Object.WriteAsync(messages, "stream");
                }
            });
            
            for (var i = 0; i < msgCount; i++) m_sut.Enqueue("a message", "channel");

            m_sut.WaitForIdle();

            Assert.True(writeHasBeenCalled);
            Assert.Equal(0, m_sut.GetPendingMessageCount());
            Assert.Equal(msgCount, m_sut.EnqueuedMessageCount);
            Assert.Equal(msgCount, m_sut.SentMessageCount);
            m_sut.Dispose();
        }

        [Fact]
        public void ChannelSpecificHandler()
        {
            var writeHasBeenCalled = false;
            const int msgCount = 30000;
            m_proxy.Setup(x => x.WriteAsync(It.IsAny<List<Message>>(), It.IsAny<string>()))
                .ReturnsAsync(new List<Message>());

            m_sut = new QueueMessageSender( m_settings, m_logger.Object);            
            m_sut.AddMessageHandler(new ChannelSpecificMessageHandler
            {
                Action = async (messages, streamName) =>
                {
                    writeHasBeenCalled = true;
                    await m_proxy.Object.WriteAsync(messages, streamName);
                }
            });
           
            for (var i = 0; i < msgCount; i++) m_sut.Enqueue("a message", "channel");

            m_sut.WaitForIdle();

            Assert.True(writeHasBeenCalled);
            Assert.Equal(0, m_sut.GetPendingMessageCount());
            Assert.Equal(msgCount, m_sut.EnqueuedMessageCount);
            Assert.Equal(msgCount, m_sut.SentMessageCount);
            m_sut.Dispose();
        }
        
        

        [Fact]
        public void LargeNumberOfMessagesWithRetries()
        {
            var messagesFailed = false;
            var writeHasBeenCalled = false;
            var tempList = new List<Message>();
            const int msgCount = 30000;
            const int failedMsgCount = 500;
            const int retryCount = 2;
            for (var i = 0; i < failedMsgCount; i++) tempList.Add(new Message {Payload = "payload"});

            var iteration = 0;
            m_proxy.Setup(x => x.WriteAsync(It.IsAny<List<Message>>(), It.IsAny<string>()))
                .ReturnsAsync(() =>
                {
                    if (iteration++ < retryCount)
                    {
                        messagesFailed = true;
                        return tempList;
                    }

                    return new List<Message>();
                });

            m_sut = new QueueMessageSender(m_settings, m_logger.Object);            
            m_sut.AddMessageHandler(new CustomMessageHandler
            {
                Func = async (messages, streamName) =>
                {
                    writeHasBeenCalled = true;
                    return await m_proxy.Object.WriteAsync(messages, streamName);
                }
            });

            for (var i = 0; i < msgCount; i++) m_sut.Enqueue("a message", "channel");

            m_sut.WaitForIdle();

            Assert.True(messagesFailed);
            Assert.True(writeHasBeenCalled);
            Assert.Equal(0, m_sut.GetPendingMessageCount());
            Assert.Equal(msgCount + retryCount * failedMsgCount, m_sut.EnqueuedMessageCount);
            Assert.Equal(msgCount, m_sut.SentMessageCount);
            m_sut.Dispose();
        }

        
        [Fact]
        public void MessagesAreSent()
        {
            var sendToKinesisHasBeenCalled = false;
            m_proxy.Setup(x => x.WriteAsync(It.IsAny<List<Message>>(), It.IsAny<string>()))
                .ReturnsAsync(new List<Message>());

            m_sut = new QueueMessageSender(m_settings, m_logger.Object);            
            m_sut.AddMessageHandler(new GlobalMessageHandler
            {
                Action = async messages =>
                {
                    sendToKinesisHasBeenCalled = true;
                    await m_proxy.Object.WriteAsync(messages, "stream");
                }
            });

            m_sut.Enqueue("a message", "channel");
            m_sut.WaitForIdle();

            Assert.True(sendToKinesisHasBeenCalled);
            Assert.Equal(0, m_sut.GetPendingMessageCount());
            m_sut.Dispose();
        }

        [Fact]
        public void MultipleThreadsToEnqueue()
        {
            var sendToKinesisHasBeenCalled = false;
            const int msgCount = 30000;
            m_proxy.Setup(x => x.WriteAsync(It.IsAny<List<Message>>(), It.IsAny<string>()))
                .ReturnsAsync(new List<Message>());

            m_sut = new QueueMessageSender(m_settings, m_logger.Object);            
            m_sut.AddMessageHandler(new GlobalMessageHandler()
            {
                Action = async messages
                    =>
                {
                    sendToKinesisHasBeenCalled = true;
                    await m_proxy.Object.WriteAsync(messages, "stream");
                }
            });
            

            Parallel.For(0, msgCount, (i, state) =>
            {
                // ReSharper disable once AccessToDisposedClosure
                m_sut.Enqueue("a message", "channel");
            });


            m_sut.WaitForIdle();
            Assert.True(sendToKinesisHasBeenCalled);
            Assert.Equal(0, m_sut.GetPendingMessageCount());
            Assert.Equal(msgCount, m_sut.EnqueuedMessageCount);
            Assert.Equal(msgCount, m_sut.SentMessageCount);
            m_sut.Dispose();
        }

        [Fact]
        public void TwoChannelsDifferentWeightManyMessages()
        {
            var sendToKinesisHasBeenCalled = false;
            const int msgCount = 10000;
            m_proxy.Setup(x => x.WriteAsync(It.IsAny<List<Message>>(), It.IsAny<string>()))
                .ReturnsAsync(new List<Message>());

            m_sut = new QueueMessageSender(m_settings, m_logger.Object);            
            m_sut.AddMessageHandler(new ChannelSpecificMessageHandler
            {
                Action = async (messages, streamName)
                    =>
                {
                    sendToKinesisHasBeenCalled = true;
                    await m_proxy.Object.WriteAsync(messages, streamName);
                }
            });

            m_sut.SetChannelWeight(1.5f, "kinesis");
            m_sut.SetChannelWeight(2, "another_kinesis");
            Parallel.For(0, msgCount, (i, state) =>
            {
                // ReSharper disable once AccessToDisposedClosure
                m_sut.Enqueue("a message", "kinesis");
            });
            Parallel.For(0, msgCount, (i, state) =>
            {
                // ReSharper disable once AccessToDisposedClosure
                m_sut.Enqueue("a message", "another_kinesis");
            });

            m_sut.WaitForIdle();
            Assert.True(sendToKinesisHasBeenCalled);
            Assert.Equal(0, m_sut.GetPendingMessageCount());
            Assert.Equal(2 * msgCount, m_sut.EnqueuedMessageCount);
            Assert.Equal(2 * msgCount, m_sut.SentMessageCount);
            m_sut.Dispose();
        }
    }
}
