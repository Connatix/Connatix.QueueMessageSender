using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QueueSender.Tests
{
    public interface IProxy : IDisposable
    {
        Task<List<QueueMessage>> WriteAsync(List<QueueMessage> messages, string streamName);
    }
}