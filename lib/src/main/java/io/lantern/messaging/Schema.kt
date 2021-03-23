package io.lantern.messaging

import com.google.protobuf.ByteString
import org.whispersystems.libsignal.DeviceId
import org.whispersystems.libsignal.ecc.ECPublicKey
import org.whispersystems.libsignal.util.Base32

object Schema {
    const val PATH_OUTBOUND = "/o"
    const val PATH_ME = "/me"
    const val PATH_CONTACTS = "/contacts"
    const val CONTACT_DIRECT_PREFIX = "d"
    const val CONTACT_GROUP_PREFIX = "g"
    const val PATH_MESSAGES = "/m"
    const val PATH_CONTACTS_BY_ACTIVITY = "/cba"
    const val PATH_CONTACT_MESSAGES = "/cm"
}

fun Model.ShortMessage.outbound(
    senderId: String,
    status: Model.ShortMessageRecord.DeliveryStatus
): Model.ShortMessageRecord {
    return Model.ShortMessageRecord.newBuilder().setSenderId(senderId).setId(id.base32)
        .setSent(sent)
        .setDirection(Model.ShortMessageRecord.Direction.OUT).setStatus(status)
        .setMessage(toByteString()).build()
}

fun Model.ShortMessage.inbound(senderId: String): Model.ShortMessageRecord {
    return Model.ShortMessageRecord.newBuilder().setSenderId(senderId).setId(id.base32)
        .setSent(sent)
        .setDirection(Model.ShortMessageRecord.Direction.IN)
        .setMessage(toByteString()).build()
}

val String.directContactPath: String
    get() = Schema.PATH_CONTACTS.path(Schema.CONTACT_DIRECT_PREFIX, this)

val String.groupContactPath: String
    get() = Schema.PATH_CONTACTS.path(Schema.CONTACT_GROUP_PREFIX, this)

val Model.ShortMessageRecord.dbPath: String
    get() = Schema.PATH_MESSAGES.path(sent, senderId, id)

val Model.ShortMessageRecord.outboundPath: String
    get() = Schema.PATH_OUTBOUND.path(sent, id)

val Model.Contact.pathSegment: String
    get() = if (type == Model.Contact.Type.DIRECT) Schema.CONTACT_DIRECT_PREFIX.path(id) else Schema.CONTACT_GROUP_PREFIX.path(
        id
    )

val Model.Contact.timestampedIdxPath: String
    get() = Schema.PATH_CONTACTS_BY_ACTIVITY.path(mostRecentMessageTime, pathSegment)

val Model.Contact.dbPath: String
    get() = Schema.PATH_CONTACTS.path(pathSegment)

fun Model.ShortMessageRecord.contactMessagePath(contact: Model.Contact): String =
    Schema.PATH_CONTACT_MESSAGES.path(
        contact.pathSegment,
        sent,
        senderId,
        id
    )

val ByteArray.base32: String get() = Base32.humanFriendly.encodeToString(this)

val ByteString.base32: String get() = Base32.humanFriendly.encodeToString(toByteArray())

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