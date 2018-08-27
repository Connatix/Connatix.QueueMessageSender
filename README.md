# Connatix.QueueMessageSender
This class provides a thread-safe mechanism to send messages to a external stream writer, such as AWS Kinesis or RabbitMQ

# Usage
1. Add the library as a package reference to any .NET Standard 2.0 compliant app
2. Create an instance of QueueMessageSender. To obtain the arguments for the constructor, use one of the approaches below.
    2.1. Create a QueueSenderAppSettings instance and supply it as argument for QueueMessageSender constructor. Also, ensure an ILogger instance is available 
    2.2. Use .net core configuration mechanism. Bind a configuration section to a QueueSenderAppSettings class. Declare a dependency injection resolver for QueueMessageSender and either resolve it or pass an IQueueMessageSender argument to the constructor of a dependency injection resolvable class.
3. Assign a send action to the instance of QueueMessageSender. Use one of the overloads of AddMessageHandler. Arguments can be of types GlobalMessageHandler, ChannelSpecificMessageHandler or CustomMessageHandler
4. Send messages to the instance of QueueMessageSender. Use Enqueue function with arguments: message and channel name



## Classes

#### QueueMessageSender

This class provides a thread-safe mechanism to send messages to a external stream writer such as AWS Kinesis or RabbitMQ

_Properties_

`SentMessageCount` This field keeps the count of all messages that were successfully sent

`EnqueuedMessageCount` This field keeps the count of the enqueued messages

`FailedMessageCount` This field keeps the count of all messages that failed to be sent

_Remarks_

If the configuration flag StatisticsEnabled is set to false then these values are 0

_Methods_

`AddMessageHandler(ChannelSpecificMessageHandler)` This function assigns a message handler for all the enqueued messages. For further details, see ChannelSpecificMessageHandler

`AddMessageHandler(CustomMessageHandler)` This function assigns a message handler for all the enqueued messages. For further details, see CustomMessageHandler

`AddMessageHandler(GlobalMessageHandler)` This function assigns a message handler for all the enqueued messages. For further details, see GlobalMessageHandler

`Enqueue(Object, String)` Enqueues a message to be delivered to a named writer stream

`SetChannelWeight(Single, String)` Each new channel has a default weight of 1. If you need to increase or decrease the threads allocated to a specific channel, use this function. The higher the number the more resources are dedicated to a channel. Bear in mind that the MaxThreadCount setting (default is 2) is still considered.

`WaitForIdle()` Waits for all the messages to be sent by the message handler

### QueueSenderAppSettings

The configuration class for the QueueMessageSender.

_Properties_

`MaxThreadCount` The max number of threads to be used by this component

`RequestsPerThread` Max number of messages to be handled by a single thread. New messages will be directed to new threads.

`StatisticsEnabled` If enabled then the QueueMessageSender records usage statistics


#### ChannelSpecificMessageHandler

This class encapsulates a write stream action for messages. Besides a list of message, the action receives as argument the channel name, thus allowing to differentiate what to do for a specific channel (write to a file, send to AWS, RabbitMQ, etc) It also encapsulates an error handler for the action. The channel is also provided as argument.

#### CustomMessageHandler

This class encapsulates a write stream action for messages. Besides a list of message, the action receives as argument the channel name, thus allowing to differentiate what to do for a specific channel (write to a file, send to AWS, RabbitMQ, etc). The action must return a list of messages that failed to be sent. If all of them were successfully sent then the action must return an empty message list. It also encapsulates an error handler for the action. The channel is also provided as argument.

#### GlobalMessageHandler

This class encapsulates a global write stream action for messages from all named channels. It also encapsulates a global error handler for the action

#### Message

This class encapsulates an enqueued message. It also tracks the retry count and the last error of the message handler action.


### Interfaces


#### IQueueMessageSender

This interface is the abstraction of the QueueMessageSender class
