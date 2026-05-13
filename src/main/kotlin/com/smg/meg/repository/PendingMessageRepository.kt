package com.smg.meg.repository

import com.smg.meg.model.document.PendingMessage
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Repository

@Repository
interface PendingMessageRepository : MongoRepository<PendingMessage, String> {
    fun findBySubscriptionIdAndBucketOrderByCreatedAtAsc(subscriptionId: String, bucket: String): List<PendingMessage>
    fun findBySubscriptionIdAndMessageIdAndBucket(subscriptionId: String, messageId: String, bucket: String): PendingMessage?
}
