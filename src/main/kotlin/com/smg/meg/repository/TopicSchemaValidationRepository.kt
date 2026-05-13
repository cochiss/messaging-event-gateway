package com.smg.meg.repository

import com.smg.meg.model.document.TopicSchemaValidation
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Repository

@Repository
interface TopicSchemaValidationRepository : MongoRepository<TopicSchemaValidation, String> {
    fun findByTopicIdAndTopicVersion(topicId: String, topicVersion: Int): TopicSchemaValidation?
    fun findByTopicIdAndTopicVersionAndEnabledTrue(topicId: String, topicVersion: Int): TopicSchemaValidation?
}
