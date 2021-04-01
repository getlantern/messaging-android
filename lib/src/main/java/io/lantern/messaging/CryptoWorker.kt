package io.lantern.messaging

import com.google.protobuf.ByteString
import io.lantern.messaging.tassis.*
import io.lantern.messaging.tassis.Callback
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
import org.whispersystems.signalservice.api.crypto.AttachmentCipherOutputStream
import org.whispersystems.signalservice.internal.util.Util
import java.io.File
import java.io.FileOutputStream
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

    private val Model.OutboundMessage.Builder.expired: Boolean get() = (nowUnixNano - sent).nanosToMillis > stopSendRetryAfterMillis

    private val Model.StoredMessage.pendingAttachments: Map<Int, Model.StoredAttachment> get() = attachmentsMap.filter { it.value.status == Model.StoredAttachment.Status.PENDING }

    private val Model.StoredMessage.allAttachmentsUploaded: Boolean get() = attachmentsMap.count { it.value.status == Model.StoredAttachment.Status.DONE } == attachmentsCount

    private val Model.OutboundMessage.Builder.knowsRecipientDevices: Boolean get() = subDeliveryStatusesCount > 0

    private val Model.OutboundMessage.Builder.recipientIdentityKey: ECPublicKey
        get() = ECPublicKey(
            this.recipientId
        )

    private val Model.StoredMessage.message: Model.Message
        get() {
            val msgBuilder =
                Model.Message.newBuilder().setId(id.fromBase32.byteString()).setText(text)
            replyToSenderId?.let { msgBuilder.setReplyToSenderId(it.fromBase32.byteString()) }
            replyToId?.let { msgBuilder.setReplyToId(it.fromBase32.byteString()) }
            attachmentsMap.forEach { (id, attachment) ->
                msgBuilder.putAttachments(id, attachment.attachment)
            }
            return msgBuilder.build()
        }

    private fun Model.OutboundMessage.Builder.deleteFailed() {
        logger.debug("deleting failed message")
        db.mutate { tx ->
            tx.delete(this.dbPath)
            val finalStatus =
                if (this.subDeliveryStatusesMap.count { it.value == Model.OutboundMessage.SubDeliveryStatus.SENT } > 0)
                    Model.StoredMessage.DeliveryStatus.PARTIALLY_FAILED else Model.StoredMessage.DeliveryStatus.COMPLETELY_FAILED
            val msgPath = this.msgPath
            tx.put(
                msgPath,
                tx.get<Model.StoredMessage>(msgPath)?.toBuilder()
                    ?.setStatus(finalStatus)?.build()
            )
        }
    }

    init {
        // immediately request some upload authorizations so that we're ready to upload attachments
        submit { getMoreUploadAuthorizationsIfNecessary() }

        // on startup, read all pending OutboundMessages to try reprocessing them
        db.list<Model.OutboundMessage>(Schema.PATH_OUTBOUND.path("%")).forEach {
            submit { processOutgoing(it.value.toBuilder()) }
        }

        // on startup, read all pending InboundAttachments to try downloading them
        db.list<Model.InboundAttachment>(Schema.PATH_INBOUND_ATTACHMENTS.path("%")).forEach {
            submit {
                val inboundAttachment = it.value
                val storedMsg = db.get<Model.StoredMessage>(inboundAttachment.msgPath)
                downloadAttachment(
                    inboundAttachment,
                    storedMsg!!.getAttachmentsOrThrow(inboundAttachment.attachmentId)
                )
            }
        }
    }

    fun processOutgoing(out: Model.OutboundMessage.Builder) {
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
                        Model.OutboundMessage.SubDeliveryStatus.SENDING
                    )
                }
                db.mutate { it.put(out.dbPath, out.build()) }
            }
        }

        db.get<Model.StoredMessage>(out.msgPath)?.let { msg ->
            val pendingAttachments = msg.pendingAttachments
            if (pendingAttachments.isNotEmpty()) {
                // handle pending attachments before sending message
                pendingAttachments.forEach { (id, attachment) ->
                    uploadAttachment(out, msg, id, attachment)
                }
                return
            }

            out.subDeliveryStatusesMap.forEach { (deviceId, status) ->
                if (status == Model.OutboundMessage.SubDeliveryStatus.SENDING) {
                    submit { encryptAndSendTo(out, deviceId, msg.message.toByteString()) }
                }
            }
        }
    }

    private fun retrievePreKeys(out: Model.OutboundMessage.Builder) {
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
        out: Model.OutboundMessage.Builder,
        deviceId: String,
        msg: ByteString
    ) {
        if (out.expired) {
            out.deleteFailed()
            return
        }

        val transferMsg =
            Model.TransferMessage.newBuilder()
                .setMessage(msg).build()
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
                            tx.get<Model.OutboundMessage>(out.dbPath)?.let {
                                val completelySent =
                                    it.subDeliveryStatusesMap.count { (_, status) -> status != Model.OutboundMessage.SubDeliveryStatus.SENT } == 1
                                if (completelySent) {
                                    // we're done
                                    tx.delete(out.dbPath)
                                } else {
                                    tx.put(
                                        out.dbPath,
                                        it.toBuilder().putSubDeliveryStatuses(
                                            deviceId,
                                            Model.OutboundMessage.SubDeliveryStatus.SENT
                                        ).build()
                                    )
                                }
                                val msgPath = out.msgPath
                                tx.get<Model.StoredMessage>(msgPath)?.let { msg ->
                                    tx.put(
                                        msgPath,
                                        msg.toBuilder()
                                            .setStatus(if (completelySent) Model.StoredMessage.DeliveryStatus.COMPLETELY_SENT else Model.StoredMessage.DeliveryStatus.PARTIALLY_SENT)
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
        out: Model.OutboundMessage.Builder,
        msg: Model.StoredMessage,
        id: Int,
        attachment: Model.StoredAttachment
    ) {
        if (out.expired) {
            out.deleteFailed()
            return
        }

        removeExpiredUploadAuthorizations()
        val auth = uploadAuthorizations.removeLastOrNull()
        if (auth == null) {
            logger.debug("getting new upload authorization before uploading attachment")
            getMoreUploadAuthorizationsIfNecessary {
                uploadAttachment(
                    out,
                    msg,
                    id,
                    attachment
                )
            }
            return
        }

        val updateStatus: (Boolean) -> Unit = { success ->
            db.mutate { tx ->
                val msgPath = out.msgPath
                tx.get<Model.StoredMessage>(msgPath)?.let { msg ->
                    val msgBuilder = msg.toBuilder()

                    val attachmentBuilder = attachment.toBuilder()
                    if (success) {
                        // TODO: be less verbose with logging like this
                        logger.debug("upload succeeded")
                        attachmentBuilder.setStatus(Model.StoredAttachment.Status.DONE)
                        attachmentBuilder.setAttachment(
                            attachmentBuilder.attachment.toBuilder()
                                .setDownloadUrl(auth.downloadURL).build()
                        )
                    } else {
                        attachmentBuilder.setStatus(Model.StoredAttachment.Status.FAILED)
                        // mark the message as failed
                        msgBuilder.setStatus(Model.StoredMessage.DeliveryStatus.COMPLETELY_FAILED)
                        // delete the outgoing short message
                        tx.delete(out.dbPath)
                        // TODO: would be nice to be able to cancel other in-flight attachment uploads to avoid wasting bandwidth here, but it's a very edge case
                    }

                    msgBuilder.putAttachments(id, attachmentBuilder.build())
                    val updatedMsg = msgBuilder.build()
                    tx.put(msgPath, updatedMsg)

                    if (updatedMsg.allAttachmentsUploaded) {
                        logger.debug("all attachments uploaded, continue with processing outgoing message")
                        submit { processOutgoing(out) }
                    }
                }
            }
        }

        if (AttachmentCipherOutputStream.getCiphertextLength(attachment.attachment.plaintextLength) > auth.maxUploadSize) {
            // TODO: cleanly handle case when attachment exceeds allowed size, including proper notification to user
            logger.error("attachment size exceeds allowed size of ${auth.maxUploadSize}, failing")
            updateStatus(false)
            return
        }

        logger.debug("uploading attachment")
        val requestBody = MultipartBody.Builder().setType(MultipartBody.FORM)
        auth.uploadFormDataMap.forEach { (key, value) ->
            requestBody.addFormDataPart(key, value)
        }
        requestBody.addFormDataPart("file", "filename", File(attachment.filePath).asRequestBody())
        val rb = requestBody.build()
        val request = Request.Builder().url(auth.uploadURL).post(rb).build()
        httpClient.newCall(request).enqueue(object : okhttp3.Callback {
            override fun onResponse(call: Call, response: Response) {
                submit {
                    try {
                        val success = response.code == 204
                        if (!success) {
                            logger.error("upload failed with unretriable status ${response.code}: ${response.body?.string()}")
                        }
                        updateStatus(success)
                    } finally {
                        response.body?.close()
                    }
                }
            }

            override fun onFailure(call: Call, e: IOException) {
                logger.error("failed to upload attachment, will try again: ${e.message}")
                retryFailed { uploadAttachment(out, msg, id, attachment) }
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
            val msg = Model.Message.parseFrom(transferMsg.message)
            val storedMsgBuilder = msg.inbound(senderId)
            // save inbound attachments and trigger downloads
            msg.attachmentsMap.forEach { (id, attachment) ->
                val inboundAttachment =
                    Model.InboundAttachment.newBuilder().setSenderId(senderId)
                        .setTs(storedMsgBuilder.ts)
                        .setMessageId(storedMsgBuilder.id).setAttachmentId(id).build()
                tx.put(inboundAttachment.dbPath, inboundAttachment)
                val storedAttachment =
                    messaging.newStoredAttachment.setAttachment(attachment).build()
                storedMsgBuilder.putAttachments(id, storedAttachment)
                downloadAttachment(inboundAttachment, storedAttachment)
            }

            // save the stored message
            val storedMsg = storedMsgBuilder.build()
            tx.put(storedMsg.dbPath, storedMsg)

            // update the Contact metadata
            val contact = messaging.updateDirectContactMetaData(
                tx,
                senderId,
                storedMsg.ts,
                Model.MessageDirection.IN,
                msg.text
            )
            // save a pointer to the message under the contact message path
            tx.put(storedMsg.contactMessagePath(contact), storedMsg.dbPath)
        }
    }

    internal fun downloadAttachment(
        inbound: Model.InboundAttachment,
        attachment: Model.StoredAttachment
    ) {
        // TODO: provide a mechanism for resumable downloads
        logger.debug("downloading attachment")
        httpClient.newCall(Request.Builder().url(attachment.attachment.downloadUrl).get().build())
            .enqueue(object : okhttp3.Callback {
                override fun onResponse(call: Call, response: Response) {
                    val success = response.code == 200
                    try {
                        if (!success) {
                            logger.error("download failed with unretriable status ${response.code}: ${response.body?.string()}")
                        } else {
                            try {
                                FileOutputStream(attachment.filePath).use { out ->
                                    Util.copy(response.body!!.byteStream(), out)
                                }
                                logger.debug("successfully downloaded attachment")
                            } catch (t: Throwable) {
                                logger.error("error downloading attachment data, will try again: ${t.message}")
                                retryFailed { downloadAttachment(inbound, attachment) }
                                return

                            }
                        }
                    } finally {
                        response.body?.close()
                    }

                    submit {
                        try {
                            val msgPath = inbound.msgPath
                            db.mutate { tx ->
                                tx.get<Model.StoredMessage>(msgPath)?.let { storedMsg ->
                                    val status =
                                        if (!success) {
                                            Model.StoredAttachment.Status.FAILED
                                        } else {
                                            Model.StoredAttachment.Status.DONE
                                        }
                                    val updatedStoredMsgBuilder = storedMsg.toBuilder()
                                    updatedStoredMsgBuilder.putAttachments(
                                        inbound.attachmentId,
                                        attachment.toBuilder().setStatus(status).build()
                                    )
                                    tx.put(msgPath, updatedStoredMsgBuilder.build())
                                    tx.delete(inbound.dbPath)
                                }
                            }
                        } finally {
                            response.body?.close()
                        }
                    }
                }

                override fun onFailure(call: Call, e: IOException) {
                    logger.error("failed to download attachment, will try again: ${e.message}")
                    retryFailed { downloadAttachment(inbound, attachment) }
                }
            })
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