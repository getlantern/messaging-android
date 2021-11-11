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

### /rkey
The recovery key from which all other keys are derived.

### /me (Model.Contact)
The contact entry for the user themselves.

### /contacts/d/[identityKey] (Model.Contact)
A direct contact, identified by their public IdentityKey

### /contacts/g/[groupId] (Model.Contact)
A group contact, identified by its group id

### /cba/[timestamp]/[d/[identityKey]|g/[groupId]]
An index of Contacts by most recent activity. A given Contact will appear only once in this index.

### /m/[senderIdentityKey]/[messageId] (Model.StoredMessage)
The full content of all Messages are stored here, including both sent and received messages.
The messageId is an id that's unique for messages sent from the given senderIdentityKey (in practice
it's a type 4 UUID).

### /cm/[d/[identityKey]|g/[groupId]]/[timestamp]/[senderIdentityKey]/[messageId]
An index of all messages for a given Contact, by the sent timestamp of the message.

### /dm/[disappearAt]/[senderIdentityKey]/[messageId]
An index of all messages that are supposed to auto disappear by some time (in unix milliseconds)

### /o/[timestamp]/[messageId] (Model.OutboundMessage)
A queue of outbound messages that are pending send. If sending to some recipients fails, messages
will be re-queued here for a limited period of time until they either send successfully or time
runs out.

### /ia/[senderIdentityKey]/[timestamp]/[messageId]/[attachmentId] (Model.InboundAttachment)
A queue of inbound attachments that are pending download. If downloading fails, downloads will be
re-queued here for a limited period of time until they either send successfully or time runs out.

### /intro/from/[fromIdentityKey]/[toIdentityKey]
An index of Introduction messages keyed to the contact who introduced us and then the contact to
whom we're being introduced.

### /intro/to/[toIdentityKey]/[fromIdentityKey]
An index of Introduction messages keyed to the contact to whom we're being introduced and then the
contact who introduced us.

### /intro/best/[toIdentityKey]
An index to the "best" introduction message for a given contact to which we're being introduced.
"Best" means most trusted (highest verification level).

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

## Entropy Issues Testing on Emulators
This library uses a lot of cryptography routines, which uses up available entropy on systems.
If you're frequently running the tests on emulators or devices which you don't actually use, the
system may run out of entropy and the tests will slow down and eventually even stop working.

The solution is to run the tests on a device or emulator on which you also use the UI, which helps
regenerate entropy.