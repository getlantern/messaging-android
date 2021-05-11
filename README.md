# messaging-android
This library provides an API to facilitate secure end-to-end encrypted messaging on Android devices,
backed by a [fork of libsignal](https://github.com/getlantern/libsignal-protocol-java/) and
communicating via a [tassis](https://github.com/getlantern/tassis) messaging server.

messaging-android is currently used in [Lantern](https://lantern.io/) but is intended to be usable
by other parties in their own clients. tassis will eventually support federation, such that 3rd
parties can host their own back end and interoperate with other messaging clients.

## Protocol Buffers
messaging-android communicates with Tassis and internally stores data using protocol buffers.
Messages exchanged with tassis are defined in [Messages.proto](messaging/src/main/protos/Messages.proto),
which is just a copy of the protocol buffers defined in [tassis](https://github.com/getlantern/tassis/blob/main/model/Messages.proto).

The messaging-android data model is defined in [Model.proto](messaging/src/main/protos/Model.proto).

## Data Model
messaging-android stores data in an [encrypted key-value store](https://github.com/getlantern/db-android/)
using keys that follow the below convention.

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
An index of all messages that are supposed to auto disappear by some time (in unix milliseconds)

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

You can install the [ktlint Intellij plugin](https://plugins.jetbrains.com/plugin/15057-ktlint-unofficial-)
for some support for linting within Android Studio.

### Add Commit Hook
./gradlew addKtlintCheckGitPreCommitHook

This adds a pre commit hook that lints all staged files upon commit.

### Manually Auto-format
./gradlew ktlintFormat

This auto-formats all Kotlin files in the project.

### Manually Check
./gradlew ktlintCheck

This manually runs the linter against all Kotlin files in the project.