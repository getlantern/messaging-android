package io.lantern.messaging

import io.lantern.db.Transaction
import io.lantern.messaging.tassis.*
import io.lantern.messaging.time.millisToNanos
import io.lantern.messaging.time.nanosToMillis
import org.signal.libsignal.metadata.SealedSessionCipher
import org.whispersystems.libsignal.DeviceId
import org.whispersystems.libsignal.SessionBuilder
import org.whispersystems.libsignal.SignalProtocolAddress
import org.whispersystems.libsignal.ecc.ECPublicKey
import org.whispersystems.libsignal.state.PreKeyBundle
import org.whispersystems.libsignal.state.PreKeyRecord
import org.whispersystems.libsignal.state.SignedPreKeyRecord

internal class CryptoWorker(
    messaging: Messaging,
    retryDelayMillis: Long,
    private val stopSendRetryAfterMillis: Long
) :
    Worker(messaging, retryDelayMillis, "crypto") {
    private val logger = messaging.logger
    private val db = messaging.db
    private val store = messaging.store
    private val cipher = SealedSessionCipher(store, store.deviceId)

    fun processOutgoing(outgoingMessagePath: String, immediate: Boolean = false) {
        var outgoingMessage = db.get<Model.OutgoingShortMessage>(outgoingMessagePath)
            ?: // must have finished sending already, ignore
            return
        val delay = if (immediate) 0L else
            if (outgoingMessage.lastFailed == 0L) 0L else
                retryDelayMillis.millisToNanos - (nowUnixNano - outgoingMessage.lastFailed)
        schedule(delay.nanosToMillis) {
            try {
                db.mutate { tx ->
                    var outgoingMessage =
                        db.get<Model.OutgoingShortMessage>(outgoingMessagePath)?.toBuilder()
                            ?: // must have finished sending already, ignore
                            return@mutate

                    if (!needsMoreProcessing(tx, outgoingMessagePath, outgoingMessage)) {
                        // we're done (either succeeded or failed, but we're not going to try anymore)
                        return@mutate
                    }

                    if (doProcessOutgoingMessage(
                            outgoingMessagePath,
                            outgoingMessage
                        )
                    ) {
                        tx.put(outgoingMessagePath, outgoingMessage.build())
                    }
                }
            } catch (t: Throwable) {
                logger.error("unexpected error processing outgoing message: ${t}")
                db.mutate { tx ->
                    tx.markOutgoingFailed(outgoingMessagePath)
                }
            }
        }
    }

    private fun doProcessOutgoingMessage(
        outgoingMessagePath: String,
        outgoingMessage: Model.OutgoingShortMessage.Builder
    ): Boolean {
        if (outgoingMessage.deviceIdsCount == 0) {
            // need to figure out which deviceIds to send to
            if (!addDeviceIds(outgoingMessagePath, outgoingMessage)) {
                // we don't have device ids yet, can't proceed
                return false
            } else {
                return true
            }
        }

        if (encryptIfNecessary(outgoingMessage)) {
            return true
        }

        sendToDevices(outgoingMessagePath, outgoingMessage)
        return false
    }

    private fun needsMoreProcessing(
        tx: Transaction,
        outgoingMessagePath: String,
        outgoingMessage: Model.OutgoingShortMessage.Builder
    ): Boolean {
        val finished =
            outgoingMessage.deviceIdsCount > 0 && outgoingMessage.perDeviceMessagesMap.values.count { it.sent } == outgoingMessage.deviceIdsCount
        val permanentlyFailed =
            !finished && outgoingMessage.firstFailed > 0 && nowUnixNano - outgoingMessage.firstFailed > stopSendRetryAfterMillis.millisToNanos
        val deliveryStatus = if (finished) {
            Model.ShortMessageRecord.DeliveryStatus.SENT
        } else if (permanentlyFailed) {
            if (outgoingMessage.perDeviceMessagesMap.values.count { it.sent } == 0) {
                Model.ShortMessageRecord.DeliveryStatus.COMPLETELY_FAILED
            } else {
                Model.ShortMessageRecord.DeliveryStatus.PARTIALLY_FAILED
            }
        } else {
            Model.ShortMessageRecord.DeliveryStatus.SENDING
        }

        val shortMessage =
            outgoingMessage.message.outbound(
                messaging.identityKeyPair.publicKey.toString(),
                deliveryStatus
            )
        tx.put(shortMessage.dbPath, shortMessage)

        if (deliveryStatus != Model.ShortMessageRecord.DeliveryStatus.SENDING) {
            // we're done (either actually finished or gave up)
            tx.delete(outgoingMessagePath)
            return false
        }

        return true
    }

    private fun addDeviceIds(
        outgoingMessagePath: String,
        outgoingMessage: Model.OutgoingShortMessage.Builder
    ): Boolean {
        val recipientIdentityKey = ECPublicKey(outgoingMessage.identityKey)
        // find out which deviceIds to send to
        val knownDeviceIds =
            store.getSubDeviceSessions(recipientIdentityKey.toString())
        if (knownDeviceIds.size > 0) {
            // we know some deviceIds for the recipient, send to the ones we know
            // TODO: figure out how to handle future additions of recipient devices (maybe retrieve preKeys periodically?)
            outgoingMessage.addAllDeviceIds(knownDeviceIds.map { it.toString() }).build()
            return true
        }

        retrievePreKeys(outgoingMessagePath, recipientIdentityKey, knownDeviceIds)
        return false
    }

    private fun retrievePreKeys(
        outgoingMessagePath: String,
        recipientIdentityKey: ECPublicKey, knownDeviceIds: List<DeviceId>
    ) {
        logger.debug("retrieving pre keys")
        val markFailed = { err: Throwable ->
            logger.debug("error retrieving pre keys, mark failed so that it will reprocess after some delay: ${err.message}")
            db.mutate { tx ->
                tx.markOutgoingFailed(outgoingMessagePath)
            }
        }

        messaging.withAnonymousClient(object : Messaging.ClientCallback<AnonymousClient> {
            override fun onClient(client: AnonymousClient) {
                client.retrievePreKeys(
                    recipientIdentityKey,
                    knownDeviceIds,
                    object : Callback<List<Messages.PreKey>> {
                        override fun onSuccess(result: List<Messages.PreKey>) {
                            logger.debug("successfully retrieved pre keys")
                            submit {
                                db.mutate {
                                    result.forEach { preKey ->
                                        val signedPreKey =
                                            SignedPreKeyRecord(preKey.signedPreKey.toByteArray())
                                        val oneTimePreKey =
                                            PreKeyRecord(preKey.oneTimePreKey.toByteArray())
                                        // TODO: implement max_recv checking for signed pre key age
                                        val builder = SessionBuilder(
                                            store,
                                            SignalProtocolAddress(
                                                recipientIdentityKey,
                                                DeviceId(preKey.deviceId.toByteArray())
                                            )
                                        )
                                        builder.process(
                                            PreKeyBundle(
                                                oneTimePreKey.id,
                                                oneTimePreKey.keyPair.publicKey,
                                                signedPreKey.id,
                                                signedPreKey.keyPair.publicKey,
                                                signedPreKey.signature,
                                                recipientIdentityKey
                                            )
                                        )
                                    }
                                }
                                processOutgoing(outgoingMessagePath, immediate = true)
                            }
                        }

                        override fun onError(err: Throwable) {
                            markFailed(err)
                        }
                    })
            }

            override fun onClientUnavailable(err: Throwable) {
                markFailed(err)
            }
        })
    }

    private fun encryptIfNecessary(
        outgoingMessage: Model.OutgoingShortMessage.Builder
    ): Boolean {
        var changed = false
        val recipientIdentityKey = ECPublicKey(outgoingMessage.identityKey)
        outgoingMessage.deviceIdsList.forEach { deviceId ->
            if (!outgoingMessage.perDeviceMessagesMap.containsKey(deviceId)) {
                val deviceMessage = Model.OutgoingShortMessageToDevice.newBuilder()
                    .setDeviceId(deviceId)
                try {
                    val transferMsg =
                        Model.TransferMessage.newBuilder()
                            .setShortMessage(outgoingMessage.message).build()
                    // TODO: we (mostly Signal) use ByteArray everywhere, but Protocol Buffers wants byte strings
                    // which have to be copied from the ByteArray. That results in a lot of extra copies,
                    // it would  sure be nice to avoid that.
                    val plainText = transferMsg.toByteArray()
                    val paddedPlainText = Padding.padMessage(plainText)
                    val to =
                        SignalProtocolAddress(recipientIdentityKey, DeviceId(deviceId))
                    val unidentifiedSenderMessage: ByteArray =
                        cipher.encrypt(to, paddedPlainText)
                    deviceMessage.setUnidentifiedSenderMessage(
                        unidentifiedSenderMessage.byteString()
                    )
                } catch (t: Throwable) {
                    // that's okay, we'll try again later
                }
                outgoingMessage.putPerDeviceMessages(deviceId, deviceMessage.build()).build()
                changed = true
            }
        }
        return changed
    }

    private fun sendToDevices(
        outgoingMessagePath: String,
        outgoingMessage: Model.OutgoingShortMessage.Builder
    ) {
        outgoingMessage.perDeviceMessagesMap.forEach { (deviceId, msg) ->
            if (!msg.sent) {
                sendToDevice(outgoingMessagePath, outgoingMessage, deviceId, msg)
            }
        }
    }

    private fun sendToDevice(
        outgoingMessagePath: String,
        outgoingMessage: Model.OutgoingShortMessage.Builder,
        deviceId: String, msg: Model.OutgoingShortMessageToDevice
    ) {
        val markFailed = {
            db.mutate { tx ->
                tx.markOutgoingFailed(outgoingMessagePath)
            }
        }

        messaging.withAnonymousClient(object : Messaging.ClientCallback<AnonymousClient> {
            override fun onClient(client: AnonymousClient) {
                val to = SignalProtocolAddress(
                    ECPublicKey(outgoingMessage.identityKey),
                    DeviceId(msg.deviceId)
                )
                val unidentifiedSenderMessage = msg.unidentifiedSenderMessage

                client.sendUnidentifiedSenderMessage(
                    to,
                    unidentifiedSenderMessage,
                    object : Callback<Unit> {
                        override fun onSuccess(result: Unit) {
                            logger.debug("successfully sent message")
                            db.mutate { tx ->
                                tx.get<Model.OutgoingShortMessage>(outgoingMessagePath)?.let {
                                    var latestOutgoingMessage =
                                        it.toBuilder().putPerDeviceMessages(
                                            deviceId,
                                            msg.toBuilder().setSent(true).build()
                                        ).build()
                                    tx.put(outgoingMessagePath, latestOutgoingMessage)
                                }
                            }
                        }

                        override fun onError(err: Throwable) {
                            markFailed()
                        }
                    })
            }

            override fun onClientUnavailable(err: Throwable) {
                markFailed()
            }
        })
    }

    internal fun decryptAndStore(inbound: InboundMessage) {
        submit {
            try {
                doDecryptAndStore(inbound)
            } catch (e: Exception) {
                logger.error("problem decrypting and storing message, dropping: ${e.message}")
                // TODO: maybe add this to a failed folder and/or a spam folder
            }
            inbound.ack()
        }
    }

    private fun doDecryptAndStore(inbound: InboundMessage) {
        db.mutate { tx ->
            val decryptionResult = cipher.decrypt(inbound.data.toByteArray())
            val plainText = Padding.stripMessagePadding(decryptionResult.paddedMessage)
            val transferMsg = Model.TransferMessage.parseFrom(plainText)
            val senderAddress = decryptionResult.senderAddress
            val senderId = senderAddress.identityKey.toString()
            if (!tx.contains(senderId.directContactPath)) {
                throw UnknownSenderException()
            }
            val msgRecord = transferMsg.shortMessage.inbound(senderId)
            val msg = Model.ShortMessage.parseFrom(msgRecord.message)
            // save the message record itself
            tx.put(msgRecord.dbPath, msgRecord)
            // update the Contact metadata
            val contact = messaging.updateDirectContactMetaData(tx, senderId, msg.sent, msg.text)
            // save a pointer to the message under the contact message path
            tx.put(msgRecord.contactMessagePath(contact), msgRecord.dbPath)
        }
    }

    internal fun registerPreKeys(numPreKeys: Int, delayMillis: Long = 0) {
        schedule(delayMillis) {
            doRegisterPreKeys(numPreKeys)
        }
    }

    private fun doRegisterPreKeys(numPreKeys: Int) {
        try {
            db.mutate {
                val spk = store.nextSignedPreKey
                val otpks = store.generatePreKeys(numPreKeys)
                messaging.withAuthenticatedClient(object :
                    Messaging.ClientCallback<AuthenticatedClient> {
                    override fun onClient(client: AuthenticatedClient) {
                        client.register(
                            spk.serialize(),
                            otpks.map { it.serialize() },
                            object : Callback<Unit> {
                                override fun onSuccess(result: Unit) {
                                    logger.debug("successfully registered pre keys")
                                }

                                override fun onError(err: Throwable) {
                                    logger.error(
                                        "failed to register pre keys: ${err.message}",
                                        err
                                    )
                                }
                            })
                    }

                    override fun onClientUnavailable(err: Throwable) {
                        logger.error("failed to register pre keys: ${err.message}", err)
                    }
                })
            }
        } catch (t: Throwable) {
            logger.debug(
                "unable to register pre keys, will try again later: ${t.message}",
                t
            )
            registerPreKeys(numPreKeys, 5000)
        }
    }
}

internal fun Transaction.markOutgoingFailed(outgoingMessagePath: String) {
    this.get<Model.OutgoingShortMessage>(outgoingMessagePath)?.let {
        val builder = it.toBuilder()
            .setLastFailed(
                nowUnixNano
            )
        if (builder.firstFailed == 0L) {
            builder.setFirstFailed(builder.lastFailed)
        }
        this.put(outgoingMessagePath, builder.build())
    }
}