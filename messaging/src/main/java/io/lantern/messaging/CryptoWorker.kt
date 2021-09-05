package io.lantern.messaging

import io.lantern.db.ChangeSet
import io.lantern.db.Subscriber
import io.lantern.db.Transaction
import io.lantern.messaging.conversions.byteString
import io.lantern.messaging.tassis.Callback
import io.lantern.messaging.tassis.InboundMessage
import io.lantern.messaging.tassis.Messages
import io.lantern.messaging.tassis.Padding
import io.lantern.messaging.time.minutesToMillis
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import okhttp3.Call
import okhttp3.MultipartBody
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.asRequestBody
import okhttp3.Response
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

/**
 * CryptoWorker handles all sending and receiving of messages.
 *
 * Outbound messages are modeled as a work queue consisting of OutboundMessages. All encryption of
 * outbound messages happens on a single worker thread, with another worker thread dedicated to
 * encrypting attachments.
 */
internal class CryptoWorker(
    messaging: Messaging,
    retryDelayMillis: Long,
    private val stopSendRetryAfterMillis: Long,
) :
    Worker(messaging, "crypto", retryDelayMillis = retryDelayMillis) {
    private val db = messaging.db
    private val store = messaging.store
    private val httpClient = OkHttpClient() // TODO: configure support for proxying and stuff
    private val cipher = SealedSessionCipher(store, store.deviceId)
    private val uploadAuthorizations = ArrayDeque<Messages.UploadAuthorization>()
    private val encryptAttachmentsExecutor = Executors.newSingleThreadExecutor()

    init {
        // find out about all disappearing messages (including previously saved ones) and schedule
        // them for deletion
        db.subscribe(
            object : Subscriber<String>(
                "disappearingMessagesSubscriber",
                Schema.PATH_DISAPPEARING_MESSAGES.path('%')
            ) {
                override fun onChanges(changes: ChangeSet<String>) {
                    changes.updates.forEach { (path, msgPath) ->
                        val scheduledTimeMillis = path.split("/")[2].toLong()
                        val delayMillis = scheduledTimeMillis - now
                        executor.schedule(
                            {
                                try {
                                    db.mutate { tx ->
                                        messaging.deleteLocally(msgPath)
                                        tx.delete(path)
                                    }
                                } catch (t: Throwable) {
                                    logger.error("failed to delete disappearing message: ${t.message}", t) // ktlint-disable max-line-length
                                }
                            },
                            delayMillis, TimeUnit.MILLISECONDS
                        )
                    }
                }
            },
            receiveInitial = true
        )

        // find out about all provisional contacts (including previously saved ones) and schedule
        // them for deletion
        db.subscribe(
            object : Subscriber<Model.ProvisionalContact>(
                "provisionalContactsSubscriber",
                Schema.PATH_PROVISIONAL_CONTACTS.path('%')
            ) {
                override fun onChanges(changes: ChangeSet<Model.ProvisionalContact>) {
                    changes.updates.forEach { (path, pc) ->
                        val delayMillis = pc.expiresAt - now
                        executor.schedule(
                            {
                                try {
                                    db.mutate { tx ->
                                        val provisionalContact =
                                            tx.get<Model.ProvisionalContact>(path)
                                        provisionalContact?.let { it ->
                                            // check the expiration again in case the provisional
                                            // contact's expiration was extended
                                            if (it.expiresAt < now) {
                                                messaging.deleteProvisionalContact(pc.contactId)
                                            }
                                        }
                                    }
                                } catch (t: Throwable) {
                                    logger.error("failed to delete provisional contact: ${t.message}") // ktlint-disable max-line-length
                                }
                            },
                            delayMillis, TimeUnit.MILLISECONDS
                        )
                    }
                }
            },
            receiveInitial = true
        )
    }

    private val Model.OutboundMessage.Builder.expired: Boolean
        get() = (now - sent) > stopSendRetryAfterMillis

    private val Model.StoredMessage.attachmentsPendingEncryption: Map<Int, Model.StoredAttachment>
        get() = attachmentsMap.filter {
            it.value.status == Model.StoredAttachment.Status.PENDING
        }

    /**
     * Any attachments that are pending upload or have thumbnails pending upload are considered
     * pending.
     */
    private val Model.StoredMessage.attachmentsPendingUpload: Map<Int, Model.StoredAttachment>
        get() = attachmentsMap.filter {
            it.value.status == Model.StoredAttachment.Status.PENDING_UPLOAD ||
                it.value.hasThumbnail() &&
                it.value.thumbnail.status == Model.StoredAttachment.Status.PENDING_UPLOAD
        }

    private val Model.StoredMessage.allAttachmentsUploaded: Boolean
        get() = attachmentsPendingUpload.isEmpty()

    private val Model.OutboundMessage.Builder.knowsRecipientDevices: Boolean
        get() = subDeliveryStatusesCount > 0

    private val Model.OutboundMessage.Builder.recipientIdentityKey: ECPublicKey
        get() = ECPublicKey(
            this.recipientId
        )

    private val Model.StoredMessage.message: Model.Message
        get() {
            val msgBuilder =
                Model.Message.newBuilder()
                    .setId(id.fromBase32.byteString())
                    .setText(text)
                    .setDisappearAfterSeconds(disappearAfterSeconds)
            replyToSenderId?.let { msgBuilder.setReplyToSenderId(it.fromBase32.byteString()) }
            replyToId?.let { msgBuilder.setReplyToId(it.fromBase32.byteString()) }
            attachmentsMap.forEach { (id, attachment) ->
                if (attachment.status != Model.StoredAttachment.Status.FAILED) {
                    val attachmentWithThumbnail = Model.AttachmentWithThumbnail.newBuilder()
                        .setAttachment(attachment.attachment)
                    if (attachment.hasThumbnail() &&
                        attachment.thumbnail.status != Model.StoredAttachment.Status.FAILED
                    ) {
                        attachmentWithThumbnail.thumbnail = attachment.thumbnail.attachment
                    }
                    msgBuilder.putAttachments(id, attachmentWithThumbnail.build())
                }
            }
            if (hasIntroduction()) {
                msgBuilder.introduction = Model.Introduction.newBuilder()
                    .setId(introduction.to.id.fromBase32.byteString())
                    .setDisplayName(introduction.displayName).build()
            }
            return msgBuilder.build()
        }

    private fun Model.OutboundMessage.Builder.deleteFailed() {
        logger.debug("deleting failed messages")
        db.mutate { tx ->
            tx.delete(this.dbPath)
            val msgPath = this.msgPath
            // update message (if it still exists)
            tx.get<Model.StoredMessage>(msgPath)?.let {
                val numSent = this.subDeliveryStatusesMap.count {
                    it.value == Model.OutboundMessage.SubDeliveryStatus.SENT
                }
                val finalStatus =
                    if (numSent > 0)
                        Model.StoredMessage.DeliveryStatus.PARTIALLY_FAILED else
                        Model.StoredMessage.DeliveryStatus.COMPLETELY_FAILED
                tx.put(
                    msgPath,
                    tx.get<Model.StoredMessage>(msgPath)?.toBuilder()
                        ?.setStatus(finalStatus)?.build()
                )
            }
        }
    }

    fun processOutbound(out: Model.OutboundMessage.Builder) {
        if (out.expired) {
            out.deleteFailed()
            return
        }

        val sendingToSelf = out.recipientId == messaging.myId.id

        if (!sendingToSelf && !out.knowsRecipientDevices) {
            val recipientIdentityKey = out.recipientIdentityKey
            // find out which deviceIds to send to
            val knownDeviceIds =
                store.getSubDeviceSessions(recipientIdentityKey.toString())
            if (knownDeviceIds.isEmpty()) {
                // we don't know any deviceIds yet, retrieve pre keys and stop processing
                retrievePreKeysForOutbound(out)
                return
            }
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

        if (out.contentCase == Model.OutboundMessage.ContentCase.MESSAGEID) {
            val msg = db.get<Model.StoredMessage>(out.msgPath)
            if (msg == null) {
                out.deleteFailed()
                return
            }

            if (msg.attachmentsPendingEncryption.isNotEmpty()) {
                encryptAttachmentsExecutor.submit {
                    db.mutate { tx ->
                        db.get<Model.StoredMessage>(out.msgPath)?.let { it ->
                            val upToDateMsg = it.toBuilder()
                            it.attachmentsPendingEncryption.forEach { (id, attachment) ->
                                val storedAttachment = attachment.toBuilder()
                                try {
                                    messaging.encryptAttachment(
                                        storedAttachment, sendingToSelf = sendingToSelf
                                    )
                                } catch (e: AttachmentPlainTextMissingException) {
                                    storedAttachment.status =
                                        Model.StoredAttachment.Status.FAILED
                                }
                                upToDateMsg.putAttachments(id, storedAttachment.build())
                            }
                            tx.put(upToDateMsg.dbPath, upToDateMsg.build())
                            submit { processOutbound(out) }
                        }
                    }
                }
                return
            }

            val attachmentsPendingUpload = msg.attachmentsPendingUpload
            if (attachmentsPendingUpload.isNotEmpty()) {
                // handle pending attachments before sending message
                attachmentsPendingUpload.forEach { (id, attachment) ->
                    if (attachment.status == Model.StoredAttachment.Status.PENDING_UPLOAD) {
                        uploadAttachment(out, msg, id, attachment, false)
                    }
                    if (attachment.hasThumbnail() &&
                        attachment.thumbnail.status ==
                        Model.StoredAttachment.Status.PENDING_UPLOAD
                    ) {
                        uploadAttachment(out, msg, id, attachment.thumbnail, true)
                    }
                }
                return
            }

            fun afterSendSuccess(tx: Transaction, completelySent: Boolean) {
                val msgPath = out.msgPath
                tx.get<Model.StoredMessage>(msgPath)?.let { msg ->
                    val msgBuilder = msg.toBuilder()
                    if (completelySent) {
                        // mark the outbound message as "viewed" only once it's been completely sent
                        messaging.markViewed(tx, msgBuilder)
                    }
                    tx.put(
                        msgPath,
                        msgBuilder
                            .setStatus(
                                if (completelySent)
                                    Model.StoredMessage.DeliveryStatus.COMPLETELY_SENT
                                else
                                    Model.StoredMessage.DeliveryStatus.PARTIALLY_SENT
                            )
                            .build()
                    )
                }
            }

            if (sendingToSelf) {
                db.mutate { tx ->
                    afterSendSuccess(tx, true)
                    tx.delete(out.dbPath)
                }
            } else {
                encryptAndSendOutboundToAll(
                    out,
                    afterSuccess = ::afterSendSuccess
                ) {
                    Model.TransferMessage.newBuilder()
                        .setMessage(msg.message.toByteString()).build()
                        .toByteArray()
                }
            }
        } else {
            val transferMsg = Model.TransferMessage.newBuilder()

            when (out.contentCase) {
                Model.OutboundMessage.ContentCase.REACTION ->
                    transferMsg.setReaction(out.reaction)
                Model.OutboundMessage.ContentCase.DELETEMESSAGEID ->
                    transferMsg.setDeleteMessageId(out.deleteMessageId)
                Model.OutboundMessage.ContentCase.DISAPPEARSETTINGS ->
                    transferMsg.setDisappearSettings(out.disappearSettings)
                Model.OutboundMessage.ContentCase.HELLO ->
                    transferMsg.setHello(out.hello)
                else -> {
                    logger.error("unknown outbound message content type")
                    return
                }
            }

            if (sendingToSelf) {
                db.mutate { tx ->
                    tx.delete(out.dbPath)
                }
            } else {
                encryptAndSendOutboundToAll(out) {
                    transferMsg.build().toByteArray()
                }
            }
        }
    }

    fun sendEphemeral(
        recipientId: String,
        transferMsg: Model.TransferMessage,
        deviceId: DeviceId? = null,
        onComplete: (MultiDeviceResult) -> Unit
    ) {
        val recipientIdentityKey = ECPublicKey(recipientId)
        val knownDeviceIds =
            store.getSubDeviceSessions(recipientIdentityKey.toString())
        if (knownDeviceIds.isNotEmpty()) {
            if (deviceId == null || knownDeviceIds.contains(deviceId)) {
                doSendEphemeral(
                    recipientIdentityKey,
                    deviceId,
                    knownDeviceIds,
                    transferMsg,
                    onComplete,
                )
                return
            }
        }

        retrievePreKeys(
            recipientIdentityKey,
            { updatedDeviceIds ->
                if (deviceId != null && !updatedDeviceIds.contains(deviceId)) {
                    MultiDeviceResult.fail(
                        onComplete,
                        RuntimeException("Unable to get pre keys for target device"),
                        deviceId = deviceId.toString()
                    )
                    return@retrievePreKeys
                }
                doSendEphemeral(
                    recipientIdentityKey,
                    deviceId,
                    knownDeviceIds,
                    transferMsg,
                    onComplete,
                )
            },
            { err -> MultiDeviceResult.fail(onComplete, err) }
        )
    }

    private fun doSendEphemeral(
        recipientIdentityKey: ECPublicKey,
        deviceId: DeviceId?,
        knownDeviceIds: List<DeviceId>,
        transferMsg: Model.TransferMessage,
        onComplete: (MultiDeviceResult) -> Unit
    ) {
        val resultBuilder = MultiDeviceResult.Builder(knownDeviceIds.size, onComplete)
        val msgBytes = transferMsg.toByteArray()
        if (deviceId != null) {
            // send to just the specific device
            encryptAndSendTo(
                recipientIdentityKey,
                deviceId,
                { msgBytes },
                { resultBuilder.deviceSucceded(deviceId.toString()) },
                { err -> resultBuilder.deviceFailed(deviceId.toString(), err) }
            )
        } else {
            if (knownDeviceIds.isEmpty()) {
                resultBuilder.fail(
                    RuntimeException("No known device ids")
                )
                return
            }

            // send to all known devices
            knownDeviceIds.forEach { knownDeviceId ->
                encryptAndSendTo(
                    recipientIdentityKey,
                    knownDeviceId,
                    { msgBytes },
                    { resultBuilder.deviceSucceded(knownDeviceId.toString()) },
                    { err -> resultBuilder.deviceFailed(knownDeviceId.toString(), err) }
                )
            }
        }
    }

    private fun retrievePreKeysForOutbound(out: Model.OutboundMessage.Builder) {
        retrievePreKeys(
            out.recipientIdentityKey,
            { processOutbound(out) },
            { err ->
                logger.debug("error retrieving pre keys: ${err.message}")
                retryFailed { processOutbound(out) }
            }
        )
    }

    private fun retrievePreKeys(
        recipientIdentityKey: ECPublicKey,
        onSuccess: (List<DeviceId>) -> Unit,
        onError: (Throwable) -> Unit
    ) {
        messaging.anonymousClientWorker.withClient { client ->
            client.requestPreKeys(
                recipientIdentityKey,
                emptyList(),
                object : Callback<List<Messages.PreKey>> {
                    override fun onSuccess(result: List<Messages.PreKey>) {
                        submit {
                            val deviceIds = ArrayList<DeviceId>()
                            db.mutate {
                                result.forEach { preKey ->
                                    val oneTimePreKey = preKey.oneTimePreKey?.let {
                                        // it's okay for oneTimePreKey to be empty
                                        if (it.size() > 0) PreKeyRecord(it.toByteArray()) else null
                                    }
                                    val signedPreKey =
                                        SignedPreKeyRecord(preKey.signedPreKey.toByteArray())
                                    val deviceId = DeviceId(preKey.deviceId.toByteArray())
                                    deviceIds.add(deviceId)
                                    // TODO: implement max_recv checking for signed pre key age
                                    val builder = SessionBuilder(
                                        store,
                                        SignalProtocolAddress(
                                            recipientIdentityKey,
                                            deviceId,
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
                            onSuccess(deviceIds)
                        }
                    }

                    override fun onError(err: Throwable) {
                        onError(err)
                    }
                }
            )
        }
    }

    private fun encryptAndSendOutboundToAll(
        out: Model.OutboundMessage.Builder,
        afterSuccess: ((Transaction, Boolean) -> Unit)? = null,
        build: () -> ByteArray
    ) {
        out.subDeliveryStatusesMap.forEach { (deviceId, status) ->
            if (status == Model.OutboundMessage.SubDeliveryStatus.SENDING) {
                submit {
                    encryptAndSendOutboundTo(out, DeviceId(deviceId), afterSuccess, build)
                }
            }
        }
    }

    private fun encryptAndSendOutboundTo(
        out: Model.OutboundMessage.Builder,
        deviceId: DeviceId,
        afterSuccess: ((Transaction, Boolean) -> Unit)? = null,
        build: () -> ByteArray
    ) {
        if (out.expired) {
            out.deleteFailed()
            return
        }

        encryptAndSendTo(
            out.recipientIdentityKey,
            deviceId,
            build,
            {
                try {
                    db.mutate { tx ->
                        // re-read message to make sure we're updating the latest
                        tx.get<Model.OutboundMessage>(out.dbPath)?.let {
                            val completelySent =
                                it.subDeliveryStatusesMap.count { (_, status) ->
                                    status != Model.OutboundMessage.SubDeliveryStatus.SENT
                                } == 1
                            if (completelySent) {
                                // we're done
                                tx.delete(out.dbPath)
                            } else {
                                tx.put(
                                    out.dbPath,
                                    it.toBuilder().putSubDeliveryStatuses(
                                        deviceId.toString(),
                                        Model.OutboundMessage.SubDeliveryStatus.SENT
                                    ).build()
                                )
                            }
                            afterSuccess?.let { it(tx, completelySent) }
                        }
                    }
                } catch (t: Throwable) {
                    logger.error("unexpected error marking successful send: ${t.message}", t)
                    retryFailed { encryptAndSendOutboundTo(out, deviceId, afterSuccess, build) }
                }
            },
            { err ->
                logger.error("failed to send: ${err.message}", err)
                retryFailed { encryptAndSendOutboundTo(out, deviceId, afterSuccess, build) }
            }
        )
    }

    private fun encryptAndSendTo(
        recipientIdentityKey: ECPublicKey,
        deviceId: DeviceId,
        build: () -> ByteArray,
        onSuccess: () -> Unit,
        onError: (Throwable) -> Unit
    ) {
        try {
            // TODO: we (mostly Signal) use ByteArray everywhere, but Protocol Buffers wants byte strings
            // which have to be copied from the ByteArray. That results in a lot of extra copies,
            // it would  sure be nice to avoid that.
            val plainText = build()
            val paddedPlainText = Padding.padMessage(plainText)
            val to =
                SignalProtocolAddress(recipientIdentityKey, deviceId)
            val unidentifiedSenderMessage: ByteArray =
                cipher.encrypt(to, paddedPlainText)

            messaging.anonymousClientWorker.withClient { client ->
                client.sendUnidentifiedSenderMessage(
                    to,
                    unidentifiedSenderMessage,
                    object : Callback<Unit> {
                        override fun onSuccess(result: Unit) {
                            onSuccess()
                        }

                        override fun onError(err: Throwable) {
                            onError(err)
                        }
                    }
                )
            }
        } catch (err: Throwable) {
            onError(err)
        }
    }

    private fun uploadAttachment(
        out: Model.OutboundMessage.Builder,
        msg: Model.StoredMessage,
        id: Int,
        attachment: Model.StoredAttachment,
        isThumbnail: Boolean
    ) {
        if (out.expired) {
            out.deleteFailed()
            return
        }

        removeExpiredUploadAuthorizations()
        val auth = uploadAuthorizations.removeLastOrNull()
        if (auth == null) {
            getMoreUploadAuthorizationsIfNecessary {
                uploadAttachment(
                    out,
                    msg,
                    id,
                    attachment,
                    isThumbnail
                )
            }
            return
        }

        val updateStatus: (Boolean) -> Unit = { success ->
            db.mutate { tx ->
                val msgPath = out.msgPath
                tx.get<Model.StoredMessage>(msgPath)?.let { msg ->
                    val msgBuilder = msg.toBuilder()
                    msgBuilder.attachmentsMap[id]?.let { readAttachment ->
                        val attachmentBuilder = if (isThumbnail)
                            readAttachment.thumbnail.toBuilder()
                        else
                            readAttachment.toBuilder()
                        if (success) {
                            attachmentBuilder.status = Model.StoredAttachment.Status.DONE
                            attachmentBuilder.attachment = attachmentBuilder.attachment.toBuilder()
                                .setDownloadUrl(auth.downloadURL).build()
                        } else {
                            attachmentBuilder.status = Model.StoredAttachment.Status.FAILED
                            if (!isThumbnail) {
                                // mark the message as failed
                                msgBuilder.status =
                                    Model.StoredMessage.DeliveryStatus.COMPLETELY_FAILED
                            }
                            // delete the outgoing short message
                            tx.delete(out.dbPath)
                            // TODO: would be nice to be able to cancel other in-flight attachment uploads to avoid wasting bandwidth here, but it's a very edge case
                        }

                        val finalAttachmentBuilder = if (isThumbnail)
                            readAttachment.toBuilder().setThumbnail(attachmentBuilder.build())
                        else
                            attachmentBuilder
                        msgBuilder.putAttachments(id, finalAttachmentBuilder.build())
                    }

                    val updatedMsg = msgBuilder.build()
                    tx.put(msgPath, updatedMsg)

                    if (updatedMsg.allAttachmentsUploaded) {
                        submit { processOutbound(out) }
                    }
                }
            }
        }

        val ciphertextLength =
            AttachmentCipherOutputStream.getCiphertextLength(attachment.attachment.plaintextLength)
        if (ciphertextLength > auth.maxUploadSize) {
            // TODO: cleanly handle case when attachment exceeds allowed size, including proper notification to user
            logger.error("attachment size exceeds allowed size of ${auth.maxUploadSize}, failing")
            updateStatus(false)
            return
        }

        val requestBody = MultipartBody.Builder().setType(MultipartBody.FORM)
        auth.uploadFormDataMap.forEach { (key, value) ->
            requestBody.addFormDataPart(key, value)
        }
        requestBody.addFormDataPart(
            "file",
            "filename",
            File(attachment.encryptedFilePath).asRequestBody()
        )
        val rb = requestBody.build()
        val request = Request.Builder().url(auth.uploadURL).post(rb).build()
        httpClient.newCall(request).enqueue(object : okhttp3.Callback {
            override fun onResponse(call: Call, response: Response) {
                submit {
                    try {
                        val success = response.code == 204
                        if (!success) {
                            logger.error("upload failed with unretriable status ${response.code}: ${response.body?.string()}") // ktlint-disable max-line-length
                        }
                        updateStatus(success)
                    } finally {
                        response.body?.close()
                    }
                }
            }

            override fun onFailure(call: Call, e: IOException) {
                logger.error("failed to upload attachment, will try again: ${e.message}", e)
                retryFailed { uploadAttachment(out, msg, id, attachment, isThumbnail) }
            }
        })
    }

    internal fun getMoreUploadAuthorizationsIfNecessary(then: () -> Unit = {}) {
        removeExpiredUploadAuthorizations()
        val numToRequest = 10 - uploadAuthorizations.size
        if (numToRequest < 0) {
            then()
            return
        }

        messaging.anonymousClientWorker.withClient { client ->
            client.requestUploadAuthorizations(
                numToRequest,
                object : Callback<List<Messages.UploadAuthorization>> {
                    override fun onSuccess(result: List<Messages.UploadAuthorization>) {
                        submit {
                            uploadAuthorizations.addAll(result)
                            then()
                        }
                    }

                    override fun onError(err: Throwable) {
                        logger.debug("error retrieving upload authorizations: ${err.message}")
                        retryFailed { getMoreUploadAuthorizationsIfNecessary() }
                    }
                }
            )
        }
    }

    private fun removeExpiredUploadAuthorizations() {
        // the 30 minute fudge factor ensures that we don't take chances with using authorizations that are near expiration
        val activeAuthorizations =
            uploadAuthorizations.filter {
                it.authorizationExpiresAt > now + 30L.minutesToMillis
            }
        uploadAuthorizations.clear()
        uploadAuthorizations.addAll(activeAuthorizations)
    }

    internal fun decryptAndStore(inbound: InboundMessage) {
        submit {
            doDecryptAndStore(inbound.data.toByteArray())
            inbound.ack()
        }
    }

    internal fun doDecryptAndStore(unidentifiedSenderMessage: ByteArray) {
        try {
            attemptDecryptAndStore(unidentifiedSenderMessage)
        } catch (e: UnknownSenderException) {
            logger.error("message from unknown sender, saving to spam")
            db.mutate { tx ->
                tx.put(
                    e.senderId.randomSpamPath,
                    unidentifiedSenderMessage
                )
            }
        } catch (e: Exception) {
            logger.error(
                "unexpected problem decrypting and storing message, dropping: ${e.message}", e
            )
        }
    }

    private fun attemptDecryptAndStore(unidentifiedSenderMessage: ByteArray) {
        db.mutate { tx ->
            val decryptionResult = cipher.decrypt(unidentifiedSenderMessage)
            val plainText = Padding.stripMessagePadding(decryptionResult.paddedMessage)
            val transferMsg = Model.TransferMessage.parseFrom(plainText)
            val senderAddress = decryptionResult.senderAddress
            val senderId = senderAddress.identityKey.toString()

            if (transferMsg.contentCase != Model.TransferMessage.ContentCase.HELLO &&
                !tx.contains(senderId.directContactPath)
            ) {
                throw UnknownSenderException(senderId)
            }

            if (transferMsg.contentCase == Model.TransferMessage.ContentCase.MESSAGE) {
                storeMessage(
                    tx,
                    senderId,
                    Model.Message.parseFrom(transferMsg.message)
                )
            } else {
                tx.get<Model.Contact>(senderId.directContactPath)?.let {
                    val updatedContact = it.toBuilder()
                        .setHasReceivedMessage(true).build()
                    tx.put(senderId.directContactPath, updatedContact)
                }

                when (transferMsg.contentCase) {
                    Model.TransferMessage.ContentCase.REACTION -> storeReaction(
                        tx,
                        senderId,
                        Model.Reaction.parseFrom(transferMsg.reaction)
                    )
                    Model.TransferMessage.ContentCase.DELETEMESSAGEID -> messaging.deleteLocally(
                        senderId.storedMessagePath(transferMsg.deleteMessageId.base32),
                        // We keep metadata so that the recipient's UI still has an empty placeholder for the deleted message.
                        // Once the recipient chooses to delete this message locally, the metadata will be deleted.
                        remotelyDeletedBy = senderId.directContactId,
                    )
                    Model.TransferMessage.ContentCase.DISAPPEARSETTINGS -> storeDisappearSettings(
                        tx,
                        senderId,
                        Model.DisappearSettings.parseFrom(transferMsg.disappearSettings)
                    )
                    Model.TransferMessage.ContentCase.HELLO -> storeHello(
                        tx,
                        senderId,
                        Model.Hello.parseFrom(transferMsg.hello)
                    )
                    Model.TransferMessage.ContentCase.WEBRTCSIGNAL -> messaging.notifyWebRTCSignal(
                        senderId,
                        senderAddress.deviceId.toString(),
                        transferMsg.webRTCSignal
                    )
                    else -> {
                        logger.debug("received currently unsupported message type")
                    }
                }
            }
        }
    }

    private fun storeMessage(tx: Transaction, senderId: String, msg: Model.Message) {
        val storedMsgBuilder = msg.inbound(senderId)

        // save the introduction if one was included
        if (msg.hasIntroduction()) {
            val toId = msg.introduction.id.directContactID.sanitized

            val matchingContact = tx.findOne<Model.Contact>(
                toId.contactPath
            )
            if (matchingContact != null) {
                logger.debug("received introduction to known contact, ignoring")
                return
            }

            val introduction = Model.IntroductionDetails.newBuilder()
                .setTo(toId)
                .setDisplayName(msg.introduction.displayName)
                .setOriginalDisplayName(msg.introduction.displayName).build()
            storedMsgBuilder.introduction = introduction

            // Delete any existing introduction message of this kind
            // This ensures that at any given time, we have at most one introduction message for
            // a given combination of from/to (i.e. each the conversation only has one introduction
            // message per distinct contact)
            tx.introductionMessage(senderId, introduction.to.id)?.let {
                messaging.deleteLocally(it.detailPath)
            }

            // Add index entries pointing to the introduction
            tx.put(
                senderId.introductionIndexPathByFrom(introduction.to.id),
                storedMsgBuilder.dbPath
            )
            tx.put(
                introduction.to.id.introductionIndexPathByTo(senderId),
                storedMsgBuilder.dbPath
            )
        }

        // save inbound attachments and trigger downloads
        msg.attachmentsMap.forEach { (id, attachmentWithThumbnail) ->
            // this is a full size attachment, store it on the message
            val fullSizeInboundAttachment =
                Model.InboundAttachment.newBuilder().setSenderId(senderId)
                    .setTs(storedMsgBuilder.ts)
                    .setMessageId(storedMsgBuilder.id).setAttachmentId(id).build()
            tx.put(fullSizeInboundAttachment.dbPath, fullSizeInboundAttachment)
            val fullSizeAttachmentBuilder =
                messaging.newStoredAttachment.setAttachment(attachmentWithThumbnail.attachment)
            if (attachmentWithThumbnail.hasThumbnail()) {
                // look for thumbnail
                val inboundThumbnail =
                    Model.InboundAttachment.newBuilder().setSenderId(senderId)
                        .setTs(storedMsgBuilder.ts)
                        .setIsThumbnail(true)
                        .setMessageId(storedMsgBuilder.id)
                        .setAttachmentId(id).build()
                tx.put(inboundThumbnail.dbPath, inboundThumbnail)
                val thumbnail =
                    messaging.newStoredAttachment
                        .setAttachment(attachmentWithThumbnail.thumbnail).build()
                fullSizeAttachmentBuilder.thumbnail = thumbnail
                downloadAttachment(inboundThumbnail, thumbnail)
            }
            val fullSizeAttachment = fullSizeAttachmentBuilder.build()
            storedMsgBuilder.putAttachments(id, fullSizeAttachment)
            downloadAttachment(fullSizeInboundAttachment, fullSizeAttachment)
        }

        // save the stored message
        val storedMsg = storedMsgBuilder.build()
        tx.put(storedMsg.dbPath, storedMsg)

        // update the Contact metadata
        messaging.updateContactMetaData(tx, storedMsg)
        // save a pointer to the message under the contact message path
        tx.put(storedMsg.contactMessagePath, storedMsg.dbPath)
    }

    private fun storeReaction(tx: Transaction, senderId: String, reaction: Model.Reaction) {
        tx.get<Model.StoredMessage>(
            reaction.reactingToSenderId.base32
                .storedMessagePath(reaction.reactingToMessageId.base32)
        )?.let { msg ->
            val builder = msg.toBuilder()
            if (reaction.emoticon.isBlank()) {
                builder.removeReactions(senderId)
            } else {
                builder.putReactions(senderId, reaction)
            }
            tx.put(msg.dbPath, builder.build())
        }
    }

    private fun storeDisappearSettings(
        tx: Transaction,
        senderId: String,
        disappearSettings: Model.DisappearSettings
    ) {
        val contactPath = senderId.directContactPath
        tx.get<Model.Contact>(contactPath)?.let { contact ->
            val updatedContactBuilder = contact.toBuilder()
                .setMessagesDisappearAfterSeconds(
                    disappearSettings.messagesDisappearAfterSeconds
                )
            if (contact.firstReceivedMessageTs == 0L) {
                updatedContactBuilder.firstReceivedMessageTs = now
            }
            tx.put(
                contactPath,
                updatedContactBuilder.build()
            )
        }
    }

    private fun storeHello(
        tx: Transaction,
        senderId: String,
        hello: Model.Hello
    ) {
        val provisionalContactPath = senderId.provisionalContactPath
        tx.get<Model.ProvisionalContact>(provisionalContactPath)?.let {
            messaging.doAddOrUpdateContact(
                senderId.directContactId,
                hello.displayName,
                mostRecentHelloTs = now
            )
            tx.delete(senderId.provisionalContactPath)
            if (!hello.final) {
                // send a hello just in case they couldn't process our first one
                messaging.sendHello(tx, senderId, final = true)
            }
        } ?: tx.get<Model.Contact>(senderId.directContactPath)?.let {
            // just update teh mostRecentHelloTs
            tx.put(
                senderId.directContactPath, it.toBuilder().setMostRecentHelloTs(now).build()
            )
            if (!hello.final) {
                messaging.sendHello(tx, senderId, final = true)
            }
        }
    }

    internal fun downloadAttachment(
        inbound: Model.InboundAttachment,
        attachment: Model.StoredAttachment
    ) {
        // TODO: provide a mechanism for resumable downloads
        httpClient.newCall(Request.Builder().url(attachment.attachment.downloadUrl).get().build())
            .enqueue(object : okhttp3.Callback {
                override fun onResponse(call: Call, response: Response) {
                    val success = response.code == 200
                    try {
                        if (!success) {
                            logger.error("download failed with un-retriable status ${response.code}: ${response.body?.string()}") // ktlint-disable max-line-length
                        } else {
                            try {
                                FileOutputStream(attachment.encryptedFilePath).use { out ->
                                    Util.copy(response.body!!.byteStream(), out)
                                }
                            } catch (t: Throwable) {
                                logger.error("error downloading attachment data, will try again: ${t.message}", t) // ktlint-disable max-line-length
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
                                tx.get<Model.StoredMessage>(msgPath)?.let { msg ->
                                    // update status
                                    val status =
                                        if (!success) {
                                            Model.StoredAttachment.Status.FAILED
                                        } else {
                                            Model.StoredAttachment.Status.DONE
                                        }
                                    val updatedMsgBuilder = msg.toBuilder()
                                    if (!inbound.isThumbnail) {
                                        // this is a regular attachment
                                        val updatedAttachment = msg
                                            .attachmentsMap[inbound.attachmentId]!!.toBuilder()
                                            .setStatus(status).build()
                                        updatedMsgBuilder.putAttachments(
                                            inbound.attachmentId,
                                            updatedAttachment
                                        )
                                    } else {
                                        // this is a thumbnail
                                        // associate thumbnail with the corresponding full-size attachment
                                        updatedMsgBuilder.attachmentsMap[inbound.attachmentId]
                                            ?.let { fullSizeAttachment ->
                                                val updatedAttachment = fullSizeAttachment
                                                    .thumbnail
                                                    .toBuilder()
                                                    .setStatus(status).build()
                                                updatedMsgBuilder.putAttachments(
                                                    inbound.attachmentId,
                                                    fullSizeAttachment.toBuilder()
                                                        .setThumbnail(updatedAttachment).build()
                                                )
                                            }
                                    }
                                    tx.put(msgPath, updatedMsgBuilder.build())
                                    tx.delete(inbound.dbPath)
                                } ?: {
                                    // message has been deleted, delete attachment
                                    File(attachment.encryptedFilePath).delete()
                                }
                            }
                        } finally {
                            response.body?.close()
                        }
                    }
                }

                override fun onFailure(call: Call, e: IOException) {
                    logger.error("failed to download attachment, will try again: ${e.message}", e)
                    retryFailed { downloadAttachment(inbound, attachment) }
                }
            })
    }

    internal fun registerPreKeys(numPreKeys: Int) {
        submit {
            doRegisterPreKeys(numPreKeys)
        }
    }

    private fun doRegisterPreKeys(numPreKeys: Int) {
        db.mutate {
            val spk = store.nextSignedPreKey
            val otpks = store.generatePreKeys(numPreKeys)
            messaging.authenticatedClientWorker.withClient { client ->
                client.register(
                    spk.serialize(),
                    otpks.map { it.serialize() },
                    object : Callback<Unit> {
                        override fun onSuccess(result: Unit) {
                            // nothing to do
                        }

                        override fun onError(err: Throwable) {
                            logger.error(
                                "failed to register pre keys: ${err.message}",
                                err
                            )
                            registerPreKeys(numPreKeys)
                        }
                    }
                )
            }
        }
    }

    override fun close() {
        encryptAttachmentsExecutor.shutdownNow()
        super.close()
    }
}
