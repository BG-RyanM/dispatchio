# dispatchio

Python message-passing framework

## Overview

This project is based on my twenty-five years of experience with message-passing or event-driven frameworks. I've worked in the game industry, in the defense industry, and in robotics. 

A common feature of such frameworks is that they provide a loose coupling between caller code and called code, compared to the tight coupling of calling a Python function directly. Events or messages can be responded to (or not) at the recipient's discretion, possibly after some amount of time has passed. Messages/events can also be broadcast to multiple recipients. As you might imagine, this can be a very useful system to have when programming a game. Games are often full of independent agents (e.g. game characters) that interact with each other and the game world. Perhaps characters in a game might respond to a message that informs them that a gunshot has been fired nearby. The characters can then change their behavior to running for cover. Other characters might not take any interest in the gunshot at all, such as a bird.

Another use for such frameworks is in the field of robotics, where systems might involve multiple processes running on different machines (or virtual machines, or containers) on a network. For developers of such a system, it's undesirable to have to think about the mechanisms for getting a message from Point A to Point B, such as the networking protocols in the middle. The developer simply wants to send and receive messages, and not have to worry about the pipes through which they pass.

A challenge of developing such systems is that communication over a network can be delayed or unreliable, so messages sent by one entity to another must be decoupled from the reply the other might send back. There must be facilities for dealing with non-received replies.  

## Basic Concepts

In this framework, messages are discrete collections of data that travel by various means between message dispatchers and message listeners, via pipelines whose implementation details are taken care of by this library. For the most basic usage, users only need to specify a destination ID or group ID, and the pipelines get the message where it needs to go. This communication can happen within a process, between processes, or even across a network. There will eventually be mechanisms that allow easy interface through GRPC, TCP/IP, Kafka, etc., with minimal setup on the part of the user. 

A message can loosely be thought of as being like a letter. The sender writes a letter, fills out the envelope with destination details, drops it in a mailbox, and it gets to where it's going via the postal delivery system. The recipient can mail a reply back to the sender's address if they like. If the postal system is unable to deliver the letter, it's returned to the sender's address.

As with letters, communication is generally asynchronous, but there are methods for synchronous communication, as well. Think of that as being more like a phone call, where you dial a number, speak your message to a person on the other end, receive their reply, then hang up. (Except it all happens instantly, much like calling a standard Python or C++ function.)

There's also the concept of group messages, which can be compared to a mailing list. A single message is sent out and copies go to all interested subscribers.

## Components

### Message dispatcher

Each process in the application has a single message dispatcher. Client code sends messages to this dispatcher, and they are passed on to the relevant message listeners.

Messages can also be passed by the local dispatcher to other dispatchers that are running in other processes. Or elsewhere on the network.

Message sending can be either synchronous or asynchronous. If the former, a response is delivered instantly, just like calling the target function directly. This can only work with recipients in the same process.

If a message is asynchronous, the dispatcher tries to get it to its destination as quickly as possible, then to return to the sender any reply that comes back.

### Message listener

Message listeners receive messages from the message dispatcher, according to the messages' specified destination. Messages can have a single recipient, or be sent to a group.

A listener must be given a unique destination ID in order for the dispatcher to recognize it. If a listener doesn't have a unique ID, it still may handle messages, but the dispatcher won't be aware of it. However, listeners can be aware of each other and chained together.

The `MessageListener` interface provides two functions that must be implemented:

* `handle_message_sync()`: handles any synchronous message that arrives from the message dispatcher. Returns a reply.
* `handle_message()`: handles any asynchronous message that arrives from the message dispatcher. Returns a reply (generally).

Users can create their own implementations of `MessageListener`, but the framework also provides a `BasicMessageListener` class, which is capable of routing messages to callback functions, to a queue, or to another listener. It's pretty versatile.

### Message

Synchronous message fields

- Message type: some human-readable name
- Message ID: unique identifier for message, chosen by sender or assigned by dispatcher
- Source ID (optional): specifies where message is coming from. Can be integer or string.
- Destination ID (optional): specifies to which listener message is going. Generally, a destination ID would be the same as some source ID, allowing for two-way communication between software entities.
- Group ID (optional): allows message to be a sent to a group of listeners that have registered themselves as subscribers to that group.
- Content

Asynchronous message fields

- Message type: some human-readable name
- Message ID: unique identifier for message, chosen by sender or assigned by dispatcher
- Source ID (optional): specifies where message is coming from. Can be integer or string.
- Destination ID (optional): specifies to which listener message is going. Generally, a destination ID would be the same as some source ID, allowing for two-way communication between software entities.
- Group ID (optional): allows message to be a sent to a group of listeners that have registered themselves as subscribers to that group.
- Process ID: if recipient is running in another process
- Response Required (optional): if set, recipient is expected to send a response
- Is Response (optional): if set, this is a response to some received message. Message ID should be the same as that of original message.
- Blocking (optional): if set, no other messages can be sent to destination/group until this one is handled. 
- Content


### Lifecycle of message

A message lives until every interested listener has had an opportunity to handle it. The system may make copies a message, rather than passing around the original code object. If a listener places a copy of a message onto a queue, it becomes the responsibility of client code to remove it from the queue.

The life of a particular message might look like:
1. User-developed code creates a message and sends it to the dispatcher
2. The dispatcher either sends the message to another process or to a local (same-process) listener
3. The listener sends the message to a callback, sends it to another listener, or places it on a queue for consumption
4. User-developer code does something with the message, then replies, if desired. The message eventually disappears.

## Basic Message Listener

The `BasicMessageListener` class contains a filtering table that specifies what should happen to message of certain characteristics. Think of it like an email filter that routes arriving emails into certain folders depending on sender, subject, and so forth. Messages go to a registered callback, into a queue, or to another chain of listeners. Filtering rules are applied in a user-specified order.

Filtering can also happen based on fields in specific message content.

### Different ways for `BasicMessageListener` to handle a message

Synchronous message
(send via synchronous `send()` function in Message Dispatcher)

| Handler          | Response from Listener in Same Process                                  | Other Process Response |
|------------------|-------------------------------------------------------------------------|------------------------|
| Callback         | Callback return data instantly returned by dispatcher `send()` function | Exception              |
| Async Callback   | Exception                                                               | Exception              |
| Message Queue    | Message placed on queue                                                 | Exception              |
| Another Listener | Message goes to other listener's synchronous handling pathway           | Exception              |

Asynchronous message
(send via asynchronous `send()` function in Message Dispatcher)

| Handler          | Response from Listener in Same Process                         | Other Process Response |
|------------------|----------------------------------------------------------------|------------------------|
| Callback         | Callback return data sent back to sender                       | Same                   |
| Async Callback   | Callback return data sent back to sender                       | Same                   |
| Message Queue    | Message placed on queue                                        | Same                   |
| Another Listener | Message goes to other listener's asynchronous handling pathway | Same                   |

## Sample Programs

`blocking_message_example.py`: Demonstrates use of blocking and non-blocking messages.       
`card_dealer.py`: Demonstrates message use in card-dealing phase of a game.   
`card_dealer_sync.py`: Same as above, but with synchronous messages.   
`deferred_reply_example.py`: Demonstrates concept of deferred replies.        

## Game (ignore this section for now)

_(For future implementation)_

Picture a two-dimensional grid. The grid is peopled by characters, each of whom occupies a single point. Characters have "types", each represented by a letter of the alphabet. The goal of each character is to wander the grid and find their "partner", another character of the same type.

Characters accomplish this goal by moving around, one cell to the north, south, east, or west per turn. They can see each other and identify each other's type, if they close within a certain distance, called `sight_radius`. Each character remembers all the other characters it has seen, and remembers their types and last known locations.

Characters can also speak to each other, if they close within a distance called `talk_radius`. If a character has knowledge about another character's partner, it will tell the other about the partner's last known location.

Characters will always pursue their partner, when they have information on the partner's whereabouts. If a character has not spoken to another within some number of game turns, the character will pursue the other until they close within talking distance.

Each character is its own `MessageListener`. There is also a `MessageListener` called the `GameManager`. Characters send messages to the game manager about their current whereabouts. Then, the game manager informs each character of other characters that are within sight, if any. Characters can then send each other talk messages.

The game manager keeps the game in sync by first sending out a "talk" message. All characters then have an opportunity to talk, if applicable. Once conversations are done, characters send their moves to the game master, which reports to individual characters on which others are visible. Then, the game master broadcasts a "move" message.
