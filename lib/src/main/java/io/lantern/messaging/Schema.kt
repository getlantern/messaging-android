package io.lantern.messaging

import com.google.protobuf.ByteString
import org.whispersystems.libsignal.DeviceId
import org.whispersystems.libsignal.ecc.ECPublicKey
import org.whispersystems.libsignal.util.Base32

object Schema {
    const val PATH_MESSAGES = "/messages"
    const val PATH_OUTBOUND = "/outbound"
    const val PATH_CONVERSATIONS = "/conversations"
    const val PATH_CONVERSATION_MESSAGES = "/conversation_messages"
}

fun Model.ShortMessage.outbound(status: Model.ShortMessageRecord.DeliveryStatus): Model.ShortMessageRecord {
    return Model.ShortMessageRecord.newBuilder().setId(this.id.base32).setSent(this.sent)
        .setDirection(Model.ShortMessageRecord.Direction.OUT).setStatus(status)
        .setMessage(this.toByteString()).build()
}

fun Model.ShortMessage.inbound(): Model.ShortMessageRecord {
    return Model.ShortMessageRecord.newBuilder().setId(this.id.base32).setSent(this.sent)
        .setDirection(Model.ShortMessageRecord.Direction.IN)
        .setMessage(this.toByteString()).build()
}

val Model.ShortMessageRecord.dbPath: String
    get() = Schema.PATH_MESSAGES.path(this.sent, this.id)

val Model.ShortMessage.dbPath: String
    get() = Schema.PATH_MESSAGES.path(this.sent, this.id)

val Model.ShortMessageRecord.outboundPath: String
    get() = Schema.PATH_OUTBOUND.path(this.sent, this.id)

val Model.Conversation.dbPath: String
    get() = Schema.PATH_CONVERSATIONS.path(
        this.mostRecentMessageTime,
        this.identityKeysList.joinToString("+")
    )

val Model.OutgoingShortMessage.conversationsQuery: String
    get() = Schema.PATH_CONVERSATIONS.path(
        "%",
        this.recipientsList.sortedWith { a, b -> a.base32.compareTo(b.base32) }.joinToString("+")
    )

fun Model.ShortMessageRecord.conversationMessagePath(identityKey: Any): String =
    Schema.PATH_CONVERSATION_MESSAGES.path(identityKey, this.sent, this.id)

val ByteArray.base32: String get() = Base32.humanFriendly.encodeToString(this)

val ByteString.base32: String get() = Base32.humanFriendly.encodeToString(this.toByteArray())

fun String.path(vararg elements: Any): String {
    val builder = StringBuilder(this)
    elements.forEach {
        builder.append("/")
        builder.append(
            when (it) {
                is ByteArray -> it.base32
                is ByteString -> it.base32
                is ECPublicKey -> it.bytes.base32
                is DeviceId -> it.bytes.base32
                else -> it
            }
        )
    }
    return builder.toString()
}