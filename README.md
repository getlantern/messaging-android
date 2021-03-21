## Data Model

### /c/[identityKey]
A direct contact, identified by their public IdentityKey

### /g/[groupId]
A group containing multiple participants, identified by a globally unique groupId (type 4 UUID).
Group participants may change over time.

### /con/[c/[contactId]|g/[groupId]]
A conversation is a stream of messages, tied either to a specific contact for one on one messages or
to a group for group messages. It is identified by either c[contactId] for a direct conversation or
g[groupId] for a group conversation.

### /cbt/[timestamp]/[c/[contactId]|g/[groupId]]
Index of Conversations by most recent message timestamp.

### /m/[timestamp]/[senderIdentityKey]/[messageId]
The full content of all ShortMessages are stored here, including both sent and received messages.
The timestamp is the sent time of the message. The messageId is an id that's unique for messages
sent from the given senderIdentityKey (in practice it's a type 4 UUID).

### /cm/[c/[contactId]|g/[groupId]]/[timestamp]/[senderIdentityKey]/[messageId]
A record of all messages for a conversation, by the sent timestamp of the message.

### /o/[messageId]
A queue of outbound messages that are pending send. If sending to some recipients fails, messages
will be re-queued here for a limited period of time until they either send successfully or time
runs out.