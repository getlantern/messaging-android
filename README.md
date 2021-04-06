## Data Model

### /me
The contact entry for the user themselves.

### /contacts/d/[identityKey]
A direct contact, identified by their public IdentityKey

### /contacts/g/[groupId]
A group contact, identified by its group id

### /cba/[timestamp]/[d/[identityKey]|g/[groupId]]
An index of Contacts by most recent activity. A given Contact will appear only once in this index.

### /m/[senderIdentityKey]/[timestamp]/[messageId]
The full content of all Messages are stored here, including both sent and received messages.
The timestamp is the sent time of the message. The messageId is an id that's unique for messages
sent from the given senderIdentityKey (in practice it's a type 4 UUID).

### /cm/[d/[identityKey]|g/[groupId]]/[timestamp]/[senderIdentityKey]/[messageId]
A record of all messages for a given Contact, by the sent timestamp of the message.

### /spam/[senderIdentityKey]/[timestamp]/[messageId]
Messages that aren't worth showing to the user for one reason or another.

### /o/[timestamp]/[messageId]
A queue of outbound messages that are pending send. If sending to some recipients fails, messages
will be re-queued here for a limited period of time until they either send successfully or time
runs out.

### /ia/[senderIdentityKey]/[timestamp]/[messageId]/[attachmentId]
A queue of inbound attachments that are pending download. If downloading fails, downloads will be
re-queued here for a limited period of time until they either send successfully or time runs out.

## Included Signal Code
The included Signal code (like AttachmentCipherInputStream) comes from https://github.com/signalapp/Signal-Android,
 not from https://github.com/signalapp/libsignal-service-java