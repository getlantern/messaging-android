## Data Model

### /me
The contact entry for the user themselves.

### /contacts/d/[identityKey]
A direct contact, identified by their public IdentityKey

### /contacts/g/[groupId]
A group contact, identified by its group id

### /cba/[timestamp]/[d/[identityKey]|g/[groupId]]
An index of Contacts by most recent activity. A given Contact will appear only once in this index.

### /m/[senderIdentityKey]/[messageId]
The full content of all Messages are stored here, including both sent and received messages.
The messageId is an id that's unique for messages sent from the given senderIdentityKey (in practice
it's a type 4 UUID).

### /cm/[d/[identityKey]|g/[groupId]]/[timestamp]/[senderIdentityKey]/[messageId]
An index of all messages for a given Contact, by the sent timestamp of the message.

### /dm/[disappearAt]/[senderIdentityKey]/[messageId]
An index of all messages that are supposed to auto disappear by some time (in unix nanos)

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
The included Signal code (like AttachmentCipherInputStream) comes from https://github.com/signalapp/Signal-Android, not from https://github.com/signalapp/libsignal-service-java

## ktlint
This project is formatted and linted with ktlint using the [ktlint-gradle plugin](https://github.com/JLLeitschuh/ktlint-gradle).

It includes a commit hook that automatically formats any staged files on commit, so there's no need
to auto-format in the IDE, and in fact the IDE's auto formatting will be different than ktlint
anyway.

You can install the [ktlint Intellij plugin](https://plugins.jetbrains.com/plugin/15057-ktlint-unofficial-)
for some support for linting within Android Studio.

### Manually Auto-format
./gradlew ktlintFormat

### Manually Check
./gradlew ktlintCheck