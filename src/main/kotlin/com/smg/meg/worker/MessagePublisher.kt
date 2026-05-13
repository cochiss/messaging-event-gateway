package com.smg.meg.worker

import com.smg.meg.controller.MessageRequest
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.stereotype.Component
import java.util.*

@Component
class MessagePublisher(private val rabbitTemplate: RabbitTemplate) {

    fun sendToExchange(topicId: String, request: MessageRequest, correlationId: String, sourceApp: String): String {
        val exchangeName = "ex.$topicId"
        val messageId = "msg_${UUID.randomUUID().toString().take(8)}"
        
        val messageBody = mapOf(
            "header" to mapOf(
                "messageId" to messageId,
                "user" to request.user,
                "correlationId" to correlationId,
                "sourceApp" to sourceApp,
                "eventType" to request.eventType,
                "eventVersion" to request.eventVersion,
                "createdAt" to java.time.Instant.now().toString()
            ),
            "payload" to request.payload
        )

        // Publica el mensaje en el exchange. 
        // El routing key "#" asegura que llegue a todas las suscripciones de esa cola.
        rabbitTemplate.convertAndSend(exchangeName, "#", messageBody)
        return messageId
    }
}