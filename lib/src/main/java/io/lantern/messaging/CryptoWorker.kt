package io.lantern.messaging

import com.google.protobuf.ByteString
import io.lantern.messaging.tassis.Callback
import io.lantern.messaging.tassis.InboundMessage
import io.lantern.messaging.tassis.Messages
import io.lantern.messaging.tassis.Padding
import io.lantern.messaging.time.millisToNanos
import io.lantern.messaging.time.minutesToMillis
import io.lantern.messaging.time.nanosToMillis
import okhttp3.*
import okhttp3.RequestBody.Companion.asRequestBody
import org.signal.libsignal.metadata.SealedSessionCipher
import org.whispersystems.libsignal.DeviceId
import org.whispersystems.libsignal.SessionBuilder
import org.whispersystems.libsignal.SignalProtocolAddress
import org.whispersystems.libsignal.ecc.ECPublicKey
import org.whispersystems.libsignal.state.PreKeyBundle
import org.whispersystems.libsignal.state.PreKeyRecord
import org.whispersystems.libsignal.state.SignedPreKeyRecord
import java.io.File
import java.io.IOException

internal class CryptoWorker(
    messaging: Messaging,
    retryDelayMillis: Long,
    private val stopSendRetryAfterMillis: Long
) :
    Worker(messaging, "crypto", retryDelayMillis = retryDelayMillis) {
    private val db = messaging.db
    private val store = messaging.store
    private val httpClient = OkHttpClient() // TODO: configure support for proxying and stuff
    private val cipher = SealedSessionCipher(store, store.deviceId)
    private val uploadAuthorizations = ArrayDeque<Messages.UploadAuthorization>()

    private val Model.OutgoingShortMessage.Builder.expired: Boolean get() = (nowUnixNano - this.sent).nanosToMillis > stopSendRetryAfterMillis

    private val Model.OutgoingShortMessage.Builder.knowsRecipientDevices: Boolean get() = this.subDeliveryStatusesCount > 0

    private val Model.OutgoingShortMessage.Builder.recipientIdentityKey: ECPublicKey
        get() = ECPublicKey(
            this.recipientId
        )

    private fun Model.OutgoingShortMessage.Builder.deleteFailed() {
        logger.debug("deleting failed message")
        db.mutate { tx ->
            tx.delete(this.dbPath)
            val finalStatus =
                if (this.subDeliveryStatusesMap.count { it.value == Model.OutgoingShortMessage.SubDeliveryStatus.SENT } > 0)
                    Model.ShortMessageRecord.DeliveryStatus.PARTIALLY_FAILED else Model.ShortMessageRecord.DeliveryStatus.COMPLETELY_FAILED
            val shortMessagePath = this.shortMessagePath
            tx.put(
                shortMessagePath,
                tx.get<Model.ShortMessageRecord>(shortMessagePath)?.toBuilder()
                    ?.setStatus(finalStatus)?.build()
            )
        }
    }

    init {
        // immediately request some upload authorizations so that we're ready to upload attachments
        submit { getMoreUploadAuthorizationsIfNecessary() }

        db.list<Model.OutgoingShortMessage>(Schema.PATH_OUTBOUND.path("%")).forEach {
            submit { processOutgoing(it.value.toBuilder()) }
        }
    }

    fun processOutgoing(out: Model.OutgoingShortMessage.Builder) {
        if (out.expired) {
            out.deleteFailed()
            return
        }

        if (!out.knowsRecipientDevices) {
            logger.debug("attempting to find recipient devices")
            val recipientIdentityKey = out.recipientIdentityKey
            // find out which deviceIds to send to
            val knownDeviceIds =
                store.getSubDeviceSessions(recipientIdentityKey.toString())
            if (knownDeviceIds.size == 0) {
                // we don't know any deviceIds yet, retrieve pre keys and stop processing
                retrievePreKeys(out)
                return
            } else {
                // we know some deviceIds for the recipient, send to the ones we know
                // TODO: figure out how to handle future additions of recipient devices (maybe retrieve preKeys periodically?)
                knownDeviceIds.forEach {
                    out.putSubDeliveryStatuses(
                        it.toString(),
                        Model.OutgoingShortMessage.SubDeliveryStatus.SENDING
                    )
                }
                db.mutate { it.put(out.dbPath, out.build()) }
            }
        }

        db.get<Model.ShortMessageRecord>(out.shortMessagePath)?.let { msgRecord ->
            out.subDeliveryStatusesMap.forEach { (deviceId, status) ->
                if (status == Model.OutgoingShortMessage.SubDeliveryStatus.SENDING) {
                    submit { encryptAndSendTo(out, deviceId, msgRecord.message) }
                }
            }

            msgRecord.attachmentsMap.forEach { (id, attachment) ->

            }
        }
    }

    private fun retrievePreKeys(out: Model.OutgoingShortMessage.Builder) {
        logger.debug("retrieving pre keys")
        val recipientIdentityKey = out.recipientIdentityKey
        messaging.anonymousClientWorker.withClient { client ->
            client.requestPreKeys(
                recipientIdentityKey,
                emptyList(),
                object : Callback<List<Messages.PreKey>> {
                    override fun onSuccess(result: List<Messages.PreKey>) {
                        logger.debug("successfully retrieved pre keys")
                        submit {
                            db.mutate {
                                result.forEach { preKey ->
                                    val oneTimePreKey = preKey.oneTimePreKey?.let {
                                        // it's okay for oneTimePreKey to be empty
                                        if (it.size() > 0) PreKeyRecord(it.toByteArray()) else null
                                    }
                                    val signedPreKey =
                                        SignedPreKeyRecord(preKey.signedPreKey.toByteArray())
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
                                            oneTimePreKey?.id ?: 0,
                                            oneTimePreKey?.keyPair?.publicKey,
                                            signedPreKey.id,
                                            signedPreKey.keyPair.publicKey,
                                            signedPreKey.signature,
                                            recipientIdentityKey
                                        )
                                    )
                                }
                            }
                            processOutgoing(out)
                        }
                    }

                    override fun onError(err: Throwable) {
                        logger.debug("error retrieving pre keys: ${err.message}")
                        retryFailed { processOutgoing(out) }
                    }
                })
        }
    }

    private fun encryptAndSendTo(
        out: Model.OutgoingShortMessage.Builder,
        deviceId: String,
        msg: ByteString
    ) {
        if (out.expired) {
            out.deleteFailed()
            return
        }

        val transferMsg =
            Model.TransferMessage.newBuilder()
                .setShortMessage(msg).build()
        // TODO: we (mostly Signal) use ByteArray everywhere, but Protocol Buffers wants byte strings
        // which have to be copied from the ByteArray. That results in a lot of extra copies,
        // it would  sure be nice to avoid that.
        val plainText = transferMsg.toByteArray()
        val paddedPlainText = Padding.padMessage(plainText)
        val to =
            SignalProtocolAddress(out.recipientIdentityKey, DeviceId(deviceId))
        val unidentifiedSenderMessage: ByteArray =
            cipher.encrypt(to, paddedPlainText)

        messaging.anonymousClientWorker.withClient { client ->
            client.sendUnidentifiedSenderMessage(
                to,
                unidentifiedSenderMessage,
                object : Callback<Unit> {
                    override fun onSuccess(result: Unit) {
                        logger.debug("successfully sent message")
                        db.mutate { tx ->
                            // re-read message to make sure we're updating the latest
                            tx.get<Model.OutgoingShortMessage>(out.dbPath)?.let {
                                val completelySent =
                                    it.subDeliveryStatusesMap.count { (_, status) -> status != Model.OutgoingShortMessage.SubDeliveryStatus.SENT } == 1
                                if (completelySent) {
                                    // we're done
                                    tx.delete(out.dbPath)
                                } else {
                                    tx.put(
                                        out.dbPath,
                                        it.toBuilder().putSubDeliveryStatuses(
                                            deviceId,
                                            Model.OutgoingShortMessage.SubDeliveryStatus.SENT
                                        ).build()
                                    )
                                }
                                val shortMessagePath = out.shortMessagePath
                                tx.get<Model.ShortMessageRecord>(shortMessagePath)?.let { msg ->
                                    tx.put(
                                        shortMessagePath,
                                        msg.toBuilder()
                                            .setStatus(if (completelySent) Model.ShortMessageRecord.DeliveryStatus.COMPLETELY_SENT else Model.ShortMessageRecord.DeliveryStatus.PARTIALLY_SENT)
                                            .build()
                                    )
                                }
                            }
                        }
                    }

                    override fun onError(err: Throwable) {
                        logger.error("failed to send: ${err.message}")
                        retryFailed { encryptAndSendTo(out, deviceId, msg) }
                    }
                })
        }
    }

    private fun uploadAttachment(
        out: Model.OutgoingShortMessage.Builder,
        id: Int,
        attachment: Model.StoredAttachment
    ) {
        if (out.expired) {
            // don't bother uploading if outgoing message has been deleted
            return
        }

        removeExpiredUploadAuthorizations()
        val auth = uploadAuthorizations.removeLastOrNull()
        if (auth == null) {
            logger.debug("getting new upload authorization before uploading attachment")
            getMoreUploadAuthorizationsIfNecessary { uploadAttachment(out, id, attachment) }
            return
        }

        logger.debug("uploading attachment")
        val requestBody = MultipartBody.Builder().setType(MultipartBody.FORM)
            .addFormDataPart("file", "filename", File(attachment.filePath).asRequestBody())
        auth.uploadFormDataMap.forEach { (key, value) ->
            requestBody.addFormDataPart(key, value)
        }
        val request = Request.Builder().url(auth.uploadURL).post(requestBody.build()).build()
        httpClient.newCall(request).enqueue(object : okhttp3.Callback {
            override fun onResponse(call: Call, response: Response) {
                submit {
                    db.mutate { tx ->
                        val attachmentStatus =
                            if (response.code != 204) {
                                logger.error("upload failed with unretriable status ${response.code}: ${response.body?.string()}")
                                Model.ShortMessageRecord.AttachmentStatus.FAILED
                            } else Model.ShortMessageRecord.AttachmentStatus.DONE
                        val shortMessagePath = out.shortMessagePath
                        tx.get<Model.ShortMessageRecord>(shortMessagePath)?.let { msg ->
                            tx.put(
                                shortMessagePath,
                                msg.toBuilder()
                                    .putAttachmentStatus(
                                        id,
                                        attachmentStatus
                                    ).build()
                            )
                        }
                    }
                }
            }

            override fun onFailure(call: Call, e: IOException) {
                logger.error("failed to upload attachment, will try again: ${e.message}")
                retryFailed { uploadAttachment(out, id, attachment) }
            }
        })
    }

    private fun getMoreUploadAuthorizationsIfNecessary(then: () -> Unit = {}) {
        removeExpiredUploadAuthorizations()
        val numToRequest = 10 - uploadAuthorizations.size
        if (numToRequest < 0) {
            then()
            return
        }

        logger.debug("requesting $numToRequest upload authorizations")
        messaging.anonymousClientWorker.withClient { client ->
            client.requestUploadAuthorizations(numToRequest,
                object : Callback<List<Messages.UploadAuthorization>> {
                    override fun onSuccess(result: List<Messages.UploadAuthorization>) {
                        logger.debug("successfully retrieved ${result.size} upload authorizations")
                        submit {
                            uploadAuthorizations.addAll(result)
                            then()
                        }
                    }

                    override fun onError(err: Throwable) {
                        logger.debug("error retrieving upload authorizations: ${err.message}")
                        retryFailed { getMoreUploadAuthorizationsIfNecessary() }
                    }
                })
        }
    }

    private fun removeExpiredUploadAuthorizations() {
        // the 30 minute fudge factor ensures that we don't take chances with using authorizations that are near expiration
        val activeAuthorizations =
            uploadAuthorizations.filter { it.authorizationExpiresAt > nowUnixNano + 30L.minutesToMillis.millisToNanos }
        uploadAuthorizations.clear()
        uploadAuthorizations.addAll(activeAuthorizations)
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
            val msg = Model.ShortMessage.parseFrom(transferMsg.shortMessage)
            val msgRecord = msg.inbound(senderId)
            // save the message record itself
            tx.put(msgRecord.dbPath, msgRecord)
            // update the Contact metadata
            val contact = messaging.updateDirectContactMetaData(
                tx,
                senderId,
                msgRecord.ts,
                Model.MessageDirection.IN,
                msg.text
            )
            // save a pointer to the message under the contact message path
            tx.put(msgRecord.contactMessagePath(contact), msgRecord.dbPath)
        }
    }

    internal fun registerPreKeys(numPreKeys: Int) {
        logger.debug("requested to register pre keys")
        submit {
            doRegisterPreKeys(numPreKeys)
        }
    }

    private fun doRegisterPreKeys(numPreKeys: Int) {
        db.mutate {
            val spk = store.nextSignedPreKey
            val otpks = store.generatePreKeys(numPreKeys)
            messaging.authenticatedClientWorker.withClient { client ->
                logger.debug("registering pre keys")
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
                            registerPreKeys(numPreKeys)
                        }
                    })
            }
        }
    }
}