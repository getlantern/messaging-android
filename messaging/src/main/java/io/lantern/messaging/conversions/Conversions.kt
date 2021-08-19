package io.lantern.messaging.conversions

import com.google.protobuf.ByteString

fun ByteArray.byteString(): ByteString {
    return ByteString.copyFrom(this)
}
