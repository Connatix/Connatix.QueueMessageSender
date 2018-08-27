using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QueueMessageSender
{
    /// <summary>
    /// This class encapsulates a global write stream action for messages from all named channels.
    /// It also encapsulates a global error handler for the action
    /// </summary>
    public class GlobalMessageHandler
    {        
        public Func<List<Message>, Task> Action { get; set; }
        #pragma warning disable 1998
        public Func<List<Message>, Task> HandleError { get; set; } = async list => { };
        #pragma warning restore 1998
    }

    /// <summary>
    /// This class encapsulates a write stream action for messages. Besides a list of message,
    /// the action receives as argument the channel name, thus allowing to differentiate what to do for a specific
    /// channel (write to a file, send to AWS, RabbitMQ, etc) 
    /// It also encapsulates an error handler for the action. The channel is also provided as argument.
    /// </summary>
    public class ChannelSpecificMessageHandler
    {        
        public Func<List<Message>, string, Task> Action { get; set; }
        #pragma warning disable 1998
        public Func<List<Message>, string, Task> HandleError { get; set; } = async (list, channelName) => { };
        #pragma warning restore 1998
    }

    /// <summary>
    /// This class encapsulates a write stream action for messages. Besides a list of message,
    /// the action receives as argument the channel name, thus allowing to differentiate what to do for a specific
    /// channel (write to a file, send to AWS, RabbitMQ, etc).
    /// The action must return a list of messages that failed to be sent. If all of them were successfully sent
    /// then the action must return an empty message list.
    /// It also encapsulates an error handler for the action. The channel is also provided as argument.
    /// </summary>
    public class CustomMessageHandler
    {
        public Func<List<Message>, string, Task<List<Message>>> Func { get; set; }
        #pragma warning disable 1998
        public Func<List<Message>, string, Task> HandleError { get; set; } = async (list, channelName) => { };
        #pragma warning restore 1998
    }
}