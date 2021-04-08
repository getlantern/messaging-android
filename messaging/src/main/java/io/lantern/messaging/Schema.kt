package io.lantern.messaging

import com.google.protobuf.ByteString
import org.whispersystems.libsignal.DeviceId
import org.whispersystems.libsignal.ecc.ECPublicKey
import org.whispersystems.libsignal.util.Base32

object Schema {
    const val PATH_CONFIG = "/cfg"
    const val PATH_OUTBOUND = "/o"
    const val PATH_INBOUND_ATTACHMENTS = "/ia"
    const val PATH_ME = "/me"
    const val PATH_CONTACTS = "/contacts"
    const val CONTACT_DIRECT_PREFIX = "d"
    const val CONTACT_GROUP_PREFIX = "g"
    const val PATH_MESSAGES = "/m"
    const val PATH_CONTACTS_BY_ACTIVITY = "/cba"
    const val PATH_CONTACT_MESSAGES = "/cm"
    const val PATH_SPAM = "/spam"
}

fun Model.Message.inbound(senderId: String): Model.StoredMessage.Builder {
    val builder = Model.StoredMessage.newBuilder().setContactId(
        Model.ContactId.newBuilder().setType(Model.ContactType.DIRECT).setId(senderId).build()
    ).setSenderId(senderId)
        .setId(id.base32)
        .setTs(nowUnixNano)
        .setDirection(Model.MessageDirection.IN)
        .setText(text)
    this.replyToSenderId?.let { builder.setReplyToSenderId(it.base32) }
    this.replyToId?.let { builder.setReplyToId(it.base32) }
    return builder
}

val Model.StoredMessage.dbPath: String
    get() = senderId.storedMessagePath(ts, id)

fun String.storedMessagePath(ts: Long, messageId: String) =
    Schema.PATH_MESSAGES.path(this, ts, messageId)

fun String.storedMessageQuery(messageId: String) =
    Schema.PATH_MESSAGES.path(this, "%", messageId)

val Model.StoredMessage.timestampUnknownQuery: String
    get() = Schema.PATH_MESSAGES.path(senderId, "%", id)

val Model.OutboundMessage.Builder.msgPath: String
    get() = senderId.storedMessagePath(sent, messageId)

val Model.OutboundMessage.Builder.dbPath: String
    get() = Schema.PATH_OUTBOUND.path(sent, id)

fun Model.StoredMessage.inboundAttachmentPath(attachmentId: Int) =
    Schema.PATH_INBOUND_ATTACHMENTS.path(senderId, ts, id, attachmentId)

val Model.InboundAttachment.dbPath: String
    get() = Schema.PATH_INBOUND_ATTACHMENTS.path(ts, senderId, messageId, attachmentId)

val Model.InboundAttachment.msgPath: String
    get() = Schema.PATH_MESSAGES.path(senderId, ts, messageId)

val Model.Contact.pathSegment: String
    get() = contactId.pathSegment

val Model.ContactId.pathSegment: String
    get() = if (type == Model.ContactType.DIRECT) Schema.CONTACT_DIRECT_PREFIX.path(id) else Schema.CONTACT_GROUP_PREFIX.path(
        id
    )

val Model.Contact.dbPath: String get() = contactId.contactPath

val Model.ContactId.contactPath: String
    get() = Schema.PATH_CONTACTS.path(pathSegment)

val String.directContactPath: String
    get() = Schema.PATH_CONTACTS.path(Schema.CONTACT_DIRECT_PREFIX, this)

val Model.Contact.timestampedIdxPath: String
    get() = Schema.PATH_CONTACTS_BY_ACTIVITY.path(mostRecentMessageTs, pathSegment)

val Model.Contact.spamQuery: String get() = Schema.PATH_SPAM.path(contactId.id, "%")

fun spamPath(senderId: String, messageId: String, ts: Long) =
    Schema.PATH_SPAM.path(senderId, ts, messageId)

val Model.StoredMessage.contactMessagePath: String
    get() =
        Schema.PATH_CONTACT_MESSAGES.path(
            contactId.pathSegment,
            ts,
            senderId,
            id
        )

val Model.StoredMessage.contactMessagesQuery: String
    get() =
        Schema.PATH_CONTACT_MESSAGES.path(contactId.pathSegment, "%")

val ByteArray.base32: String get() = Base32.humanFriendly.encodeToString(this)

val ByteString.base32: String get() = Base32.humanFriendly.encodeToString(toByteArray())

val String.fromBase32: ByteArray get() = Base32.humanFriendly.decodeFromString(this)

fun String.path(vararg elements: Any): String {
    val builder = StringBuilder(this)
    elements.forEach {
        builder.append("/")
        builder.append(
            when (it) {
                is ByteArray -> it.base32
                is ByteString -> it.base32
                is ECPublicKey -> it.toString()
                is DeviceId -> it.toString()
                else -> it
            }
        )
    }
    return builder.toString()
}