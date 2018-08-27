# Connatix.QueueMessageSender
This class provides a thread-safe mechanism to send messages to a external stream writer, such as AWS Kinesis or RabbitMQ

# Usage
1. Add the library as a package reference to any .NET Standard 2.0 compliant app
2. Create an instance of QueueMessageSender. To obtain the arguments for the constructor, use one of the approaches below.
    2.1. Create a QueueSenderAppSettings instance and supply it as argument for QueueMessageSender constructor. Also, ensure an ILogger instance is available 
    2.2. Use .net core configuration mechanism. Bind a configuration section to a QueueSenderAppSettings class. Declare a dependency injection resolver for QueueMessageSender and either resolve it or pass an IQueueMessageSender argument to the constructor of a dependency injection resolvable class.
3. Assign a send action to an instance of QueueMessageSender. Use one of the overloads of AddMessageHandler. Arguments can be of types GlobalMessageHandler, ChannelSpecificMessageHandler or CustomMessageHandler
4. 