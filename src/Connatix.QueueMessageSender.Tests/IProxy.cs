using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QueueMessageSender.Tests
{
    public interface IProxy : IDisposable
    {
        Task<List<Message>> WriteAsync(List<Message> messages, string streamName);
    }
}