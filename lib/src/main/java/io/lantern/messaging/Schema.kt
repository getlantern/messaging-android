package io.lantern.messaging

import com.google.protobuf.ByteString
import org.whispersystems.libsignal.DeviceId
import org.whispersystems.libsignal.ecc.ECPublicKey
import org.whispersystems.libsignal.util.Base32

object Schema {
    const val PATH_OUTBOUND = "/o"
    const val PATH_CONTACTS = "/c"
    const val PATH_GROUPS = "/g"
    const val PATH_MESSAGES = "/m"
    const val PATH_CONVERSATIONS = "/con"
    const val PATH_CONVERSATION_MESSAGES = "/cm"
}

fun Model.ShortMessage.outbound(
    senderId: String,
    status: Model.ShortMessageRecord.DeliveryStatus
): Model.ShortMessageRecord {
    return Model.ShortMessageRecord.newBuilder().setSenderId(senderId).setId(this.id.base32)
        .setSent(this.sent)
        .setDirection(Model.ShortMessageRecord.Direction.OUT).setStatus(status)
        .setMessage(this.toByteString()).build()
}

fun Model.ShortMessage.inbound(senderId: String): Model.ShortMessageRecord {
    return Model.ShortMessageRecord.newBuilder().setSenderId(senderId).setId(this.id.base32)
        .setSent(this.sent)
        .setDirection(Model.ShortMessageRecord.Direction.IN)
        .setMessage(this.toByteString()).build()
}

val Model.ShortMessageRecord.dbPath: String
    get() = Schema.PATH_MESSAGES.path(this.sent, this.senderId, this.id)

val Model.ShortMessageRecord.outboundPath: String
    get() = Schema.PATH_OUTBOUND.path(this.sent, this.id)

val Model.OutgoingShortMessage.conversationPath: String
    get() = Schema.PATH_CONVERSATIONS.path(
        this.message.sent,
        if (this.contactId != "") "c".path(this.contactId) else "g".path(this.groupId)
    )

fun Model.ShortMessageRecord.conversationPath(contactId: String): String =
    Schema.PATH_CONVERSATIONS.path(this.sent, "c".path(contactId))

val Model.Conversation.partyPath: String
    get() = if (this.contactId != "") "c".path(this.contactId) else "g".path(this.groupId)

fun Model.ShortMessageRecord.conversationMessagePath(conversation: Model.Conversation): String =
    Schema.PATH_CONVERSATION_MESSAGES.path(
        conversation.partyPath,
        this.sent,
        this.senderId,
        this.id
    )

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
                is ECPublicKey -> it.toString()
                is DeviceId -> it.toString()
                else -> it
            }
        )
    }
    return builder.toString()
}