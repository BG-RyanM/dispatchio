# dispatchio

Python message-passing framework

## Overview

Messages are discrete collections of data that travel by various means between message dispatchers and message listeners, via pipelines whose implementation details are taken care of by this library. Users only need to specify a source ID and a destination ID, and the pipelines will get the messages where they need to go. This communication can happen within a process, between processes, or even across a network.

A message can loosely be thought of as being like a letter. The sender writes a letter, fills out the envelope with address details, drops it in a mailbox, and it gets to its destination via the postal delivery system. The recipient can mail a reply back to the sender's address if they like.

As with letters, communication is generally asynchronous, but there are methods for synchronous communication, as well. Think of that as being more like a phone call, where you dial a number, speak your message to a person on the other end, receive their reply, then hang up.

## Components

### Message dispatcher

Each process in the application has a single message dispatcher. Client code sends messages to this dispatcher and they are passed on to the relevent message listeners.

Messages can also be passed to other dispatchers that are running in other processes.

Message sending is asynchronous. The dispatcher will always try to get the message to the recipient as an atomic operation, if possible, but it's up to the recipient how to handle it.

Clients of a message dispatcher can request a unique source ID or choose and register their own (if not already taken).

### Message listener

Message listeners receive messages from the message dispatcher, according to the type of message or its destination. Once a message is received, the listener can pass it on to a registered callback function, place it on a queue for later access, or send it to another listener.

A listener must be given a unique destination ID in order for the dispatcher to recognize it. Usually, this ID should be the same as a source ID registered with the message dispatcher. If a listener doesn't have a unique ID, it still may handle messages, but the dispatcher won't be aware of it. However, listeners can be aware of each other and chained together.

### Message

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

### A typical listener

- Contains filtering table which specifies what should happen to message of certain characteristics. Think of it like an email filter that routes arriving emails into certain folders depending on sender, subject, and so forth. Messages go to a registered callback, into a queue, or to another chain of listeners
- Listeners can be customized by subclassing from main listener class. (Concept of pre-filter, post-filter functions.)
- Filtering table can also filter upon customer message fields

## Game

Picture a two-dimensional grid. The grid is peopled by characters, each of whom occupies a single point. Characters have "types", each represented by a letter of the alphabet. The goal of each character is to wander the grid and find their "partner", another character of the same type.

Characters accomplish this goal by moving around, one cell to the north, south, east, or west per turn. They can see each other and identify each other's type, if they close within a certain distance, called `sight_radius`. Each character remembers all the other characters it has seen, and remembers their types and last known locations.

Characters can also speak to each other, if they close within a distance called `talk_radius`. If a character has knowledge about another character's partner, it will tell the other about the partner's last known location.

Characters will always pursue their partner, when they have information on the partner's whereabouts. If a character has not spoken to another within some number of game turns, the character will pursue the other until they close within talking distance.

Each character is its own `MessageListener`. There is also a `MessageListener` called the `GameManager`. Characters send messages to the game manager about their current whereabouts. Then, the game manager informs each character of other characters that are within sight, if any. Characters can then send each other talk messages.

The game manager keeps the game in sync by first sending out a "talk" message. All characters then have an opportunity to talk, if applicable. Once conversations are done, characters send their moves to the game master, which reports to individual characters on which others are visible. Then, the game master broadcasts a "move" message.
