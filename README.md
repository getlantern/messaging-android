## Data Model

### /messages/[timestamp]/[messageid]
The full content of all ShortMessages are stored here, including both sent and received messages.
The timestamp is the sent time of the message and the messageid is a globally unique identifier for
the message.

### /conversations/[timestamp]/[conversationid]
A conversation is defined as the

/conversationmessages/[timestamp]/[messageid]