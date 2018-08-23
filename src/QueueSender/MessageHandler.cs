using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QueueSender
{
    public class GlobalMessageHandler
    {        
        public Func<List<QueueMessage>, Task> Action { get; set; }
        public Func<List<QueueMessage>, Task> HandleError { get; set; } = async list => { };
    }

    public class ChannelSpecificMessageHandler
    {        
        public Func<List<QueueMessage>, string, Task> Action { get; set; }
        public Func<List<QueueMessage>, string, Task> HandleError { get; set; } = async (list, channelName) => { };
    }

    public class CustomMessageHandler
    {
        public Func<List<QueueMessage>, string, Task<List<QueueMessage>>> Func { get; set; }
        public Func<List<QueueMessage>, string, Task> HandleError { get; set; } = async (list, channelName) => { };
    }
}