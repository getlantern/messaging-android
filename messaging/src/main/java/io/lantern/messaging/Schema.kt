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
    val builder = Model.StoredMessage.newBuilder().setSenderId(senderId).setId(id.base32)
        .setTs(nowUnixNano)
        .setDirection(Model.MessageDirection.IN)
        .setText(text)
    this.replyToSenderId?.let { builder.setReplyToSenderId(it.base32) }
    this.replyToId?.let { builder.setReplyToId(it.base32) }
    return builder
}

val String.directContactPath: String
    get() = Schema.PATH_CONTACTS.path(Schema.CONTACT_DIRECT_PREFIX, this)

val String.groupContactPath: String
    get() = Schema.PATH_CONTACTS.path(Schema.CONTACT_GROUP_PREFIX, this)

val Model.StoredMessage.dbPath: String
    get() = Schema.PATH_MESSAGES.path(senderId, ts, id)

val Model.StoredMessage.timestampUnknownQuery: String
    get() = Schema.PATH_MESSAGES.path(senderId, "%", id)

val Model.OutboundMessage.Builder.msgPath: String
    get() = Schema.PATH_MESSAGES.path(senderId, sent, id)

val Model.OutboundMessage.Builder.dbPath: String
    get() = Schema.PATH_OUTBOUND.path(sent, id)

val Model.StoredMessage.outboundPath: String
    get() = Schema.PATH_OUTBOUND.path(ts, id)

fun Model.StoredMessage.inboundAttachmentPath(attachmentId: Int) =
    Schema.PATH_INBOUND_ATTACHMENTS.path(senderId, ts, id, attachmentId)

val Model.InboundAttachment.dbPath: String
    get() = Schema.PATH_INBOUND_ATTACHMENTS.path(ts, senderId, messageId, attachmentId)

val Model.InboundAttachment.msgPath: String
    get() = Schema.PATH_MESSAGES.path(senderId, ts, messageId)

val Model.Contact.pathSegment: String
    get() = if (type == Model.Contact.Type.DIRECT) Schema.CONTACT_DIRECT_PREFIX.path(id) else Schema.CONTACT_GROUP_PREFIX.path(
        id
    )

val Model.Contact.timestampedIdxPath: String
    get() = Schema.PATH_CONTACTS_BY_ACTIVITY.path(mostRecentMessageTs, pathSegment)

val Model.Contact.spamQuery: String get() = Schema.PATH_SPAM.path(id, "%")

fun spamPath(senderId: String, messageId: String, ts: Long) =
    Schema.PATH_SPAM.path(senderId, ts, messageId)

fun Model.StoredMessage.contactMessagePath(contact: Model.Contact): String =
    Schema.PATH_CONTACT_MESSAGES.path(
        contact.pathSegment,
        ts,
        senderId,
        id
    )

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