package io.lantern.messaging

import com.google.protobuf.ByteString
import io.lantern.db.Detail
import io.lantern.db.Queryable
import io.lantern.db.Raw
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
    const val PATH_DISAPPEARING_MESSAGES = "/dm"
    const val PATH_SPAM = "/spam"
    const val PATH_INTRODUCTIONS_BY_FROM = "/intro/from"
    const val PATH_INTRODUCTIONS_BY_TO = "/intro/to"
    const val PATH_PROVISIONAL_CONTACTS = "/pc"
}

fun Model.Message.inbound(senderId: String): Model.StoredMessage.Builder {
    // TODO: need to support group messages
    val builder = Model.StoredMessage.newBuilder().setContactId(senderId.directContactId)
        .setSenderId(senderId)
        .setId(id.base32)
        .setTs(now)
        .setDirection(Model.MessageDirection.IN)
        .setText(text)
        .setDisappearAfterSeconds(disappearAfterSeconds)
    replyToSenderId?.let { builder.setReplyToSenderId(it.base32) }
    replyToId?.let { builder.setReplyToId(it.base32) }
    return builder
}

val Model.StoredMessage.dbPath: String
    get() = senderId.storedMessagePath(id)

val Model.StoredMessage.Builder.dbPath: String
    get() = senderId.storedMessagePath(id)

val String.directContactId: Model.ContactId
    get() = Model.ContactId.newBuilder().setType(Model.ContactType.DIRECT).setId(this).build()

val ByteString.directContactID: Model.ContactId
    get() = base32.directContactId

val String.provisionalContactPath: String
    get() = Schema.PATH_PROVISIONAL_CONTACTS.path(this)

fun String.storedMessagePath(messageId: String) =
    Schema.PATH_MESSAGES.path(this, messageId)

val Model.OutboundMessage.Builder.msgPath: String
    get() = senderId.storedMessagePath(messageId)

val Model.OutboundMessage.Builder.dbPath: String
    get() = Schema.PATH_OUTBOUND.path(sent, id)

val Model.InboundAttachment.dbPath: String
    get() = Schema.PATH_INBOUND_ATTACHMENTS.path(ts, senderId, messageId, attachmentId, isThumbnail)

val Model.InboundAttachment.msgPath: String
    get() = Schema.PATH_MESSAGES.path(senderId, messageId)

val Model.Contact.pathSegment: String
    get() = contactId.pathSegment

val Model.ContactId.pathSegment: String
    get() = if (type == Model.ContactType.DIRECT)
        Schema.CONTACT_DIRECT_PREFIX.path(id)
    else Schema.CONTACT_GROUP_PREFIX.path(id)

val Model.Contact.dbPath: String get() = contactId.contactPath

val Model.ContactId.contactPath: String
    get() = Schema.PATH_CONTACTS.path(pathSegment)

val String.directContactPath: String
    get() = Schema.PATH_CONTACTS.path(Schema.CONTACT_DIRECT_PREFIX, this)

val Model.Contact.timestampedIdxPath: String
    get() = Schema.PATH_CONTACTS_BY_ACTIVITY.path(mostRecentMessageTs, pathSegment)

val Model.ContactId.contactByActivityQuery: String
    get() = Schema.PATH_CONTACTS_BY_ACTIVITY.path("%", pathSegment)

val Model.Contact.spamQuery: String get() = contactId.spamQuery

val Model.ContactId.spamQuery: String get() = Schema.PATH_SPAM.path(id, "%")

val String.randomSpamPath: String
    get() = Schema.PATH_SPAM.path(this, now, randomMessageId.base32)

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
        contactId.contactMessagesQuery

val Model.ContactId.contactMessagesQuery: String
    get() = Schema.PATH_CONTACT_MESSAGES.path(pathSegment, '%')

val Model.StoredMessage.Builder.disappearingMessagePath: String
    get() =
        Schema.PATH_DISAPPEARING_MESSAGES.path(
            disappearAt,
            senderId,
            id
        )

fun String.introductionIndexPathByFrom(toId: String) =
    Schema.PATH_INTRODUCTIONS_BY_FROM.path(this, toId)

fun String.introductionIndexPathByTo(fromId: String) =
    Schema.PATH_INTRODUCTIONS_BY_TO.path(this, fromId)

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

fun Queryable.introductionMessagesTo(to: String): List<Detail<Model.StoredMessage>> =
    listDetails(Schema.PATH_INTRODUCTIONS_BY_TO.path(to, "%"))

fun Queryable.introductionMessage(from: String, to: String): Detail<Raw<Model.StoredMessage>>? =
    listDetailsRaw<Model.StoredMessage>(from.introductionIndexPathByFrom(to)).firstOrNull()

val Model.Contact.fullText: String
    get() = "$displayName $contactId.id"

val Model.StoredMessage.fullText: String?
    get() = text
