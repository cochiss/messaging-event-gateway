package com.smg.meg.repository

import com.smg.meg.model.document.Topic
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Repository

@Repository
interface TopicRepository : MongoRepository<Topic, String>