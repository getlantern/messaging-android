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
    const val PATH_CONVERSATIONS_BY_TIMESTAMP = "/cbt"
    const val PATH_CONVERSATION_MESSAGES = "/cm"
}

val String.contactPath: String
    get() = Schema.PATH_CONTACTS.path(this)

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

val Model.ShortMessageRecord.dbPath: String
    get() = Schema.PATH_MESSAGES.path(sent, senderId, id)

val Model.ShortMessageRecord.outboundPath: String
    get() = Schema.PATH_OUTBOUND.path(sent, id)

val String.contactConversationPath get() = Schema.PATH_CONVERSATIONS.path("c", this)

val Model.Conversation.partyPath: String
    get() = if (contactId != "") "c".path(contactId) else "g".path(groupId)

val Model.Conversation.timestampedIdxPath: String
    get() = Schema.PATH_CONVERSATIONS_BY_TIMESTAMP.path(mostRecentMessageTime, partyPath)

val Model.Conversation.dbPath: String
    get() = Schema.PATH_CONVERSATIONS.path(partyPath)

fun Model.ShortMessageRecord.conversationMessagePath(conversation: Model.Conversation): String =
    Schema.PATH_CONVERSATION_MESSAGES.path(
        conversation.partyPath,
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