# dispatchio

Python message-passing framework

Messages are collections of data that travel by various means between message dispatchers and message listeners, via pipelines that are created beneath the hood of the system. Users only need to specify a source ID and a destination ID, and the pipelines will get the messages where they need to go.

Message dispatcher
--------

Each process in the application has a single message dispatcher. Client code sends messages to this dispatcher and they are passed on to the relevent message listeners.

Messages can also be passed to other dispatchers that are running in other processes.

Message sending is asynchronous. The dispatcher will always try to get the message to the recipient as an atomic operation, if possible, but it's up to the recipient how to handle it.

Clients of a message dispatcher can request a unique source ID or choose and register their own (if not already taken).

Message listener
--------

Message listeners receive messages from the message dispatcher, according to the type of message or its destination. Once a message is received, the listener can pass it on to a registered callback function, place it on a queue for later access, or send it to another listener.

A listener must be given a unique destination ID in order for the dispatcher to recognize it. Usually, this ID should be the same as a source ID registered with the message dispatcher. If a listener doesn't have a unique ID, it still may handle messages, but the dispatcher won't be aware of it. However, listeners can be aware of each other and chained together.

Message
--------

Message fields

- Message type: some human-readable name
- Message ID: unique identifier for message, chosen by sender or assigned by dispatcher
- Source ID (optional): specifies where message is coming from. Can be integer or string.
- Destination ID (optional): specifies to which listener message is going. Generally, a destination ID would be the same as some source ID, allowing for two-way communication between software entities.
- Group ID (optional): allows message to be a sent to a group of listeners that have registered themselves as subscribers to that group.
- Response Required (optional): if set, recipient is expected to send a response
- Is Response (optional): if set, this is a response to some received message. Message ID should be the same.
- Synchronous (optional): message must be sent and responded to synchronously. If not possible, an exception will be raised.
- Content

Lifecycle of message

A message lives until every interested listener has had an opportunity to handle it. The system may make copies a message, rather than passing around the original code object. If a listener places a copy of a message onto a queue, it becomes the responsibility of client code to remove it from the queue.

A typical listener
--------

- Contains routing table which specifies what should happen to message of certain characteristics. Think of it like an email filter that routes arriving emails into certain folders depending on sender, subject, and so forth. Messages go to a registered callback, into a queue, or to another chain of listeners
- Listeners can be customized by subclassing from main listener class. (Concept of pre-filter, post-filter functions.)
- Routing table can also filter upon customer message fields
