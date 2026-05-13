package com.smg.meg.integration

import com.smg.meg.controller.MessageRequest
import com.smg.meg.controller.SubscriptionRequest
import com.smg.meg.service.MessageService
import com.smg.meg.service.TopicService
import com.smg.meg.service.SubscriptionService
import com.smg.meg.worker.PushDispatchScheduler
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.test.context.TestPropertySource
import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.UUID
import java.util.concurrent.TimeUnit

@SpringBootTest
@Testcontainers
@TestPropertySource(
    properties = [
        "meg.push.max-delivery-attempts=3",
        // Evita ticks del scheduler concurrentes con dispatchPushMessages() manual (60 s >> duración del test).
        "meg.push.dispatch-interval-ms=60000",
        // PULL: segundo pull tras ACK debe ver el otro mensaje (visibilidad corta en IT).
        "meg.pull.visibility-timeout-seconds=1"
    ]
)
class PullAckFlowIntegrationTest {

    @Autowired
    private lateinit var topicService: TopicService

    @Autowired
    private lateinit var subscriptionService: SubscriptionService

    @Autowired
    private lateinit var messageService: MessageService

    @Autowired
    private lateinit var pushDispatchScheduler: PushDispatchScheduler

    @Autowired
    private lateinit var rabbitTemplate: RabbitTemplate

    @Test
    fun `pull and ack only one message keeps others pending`() {
        val topicId = "reintegros-${UUID.randomUUID().toString().take(6)}"
        val topic = topicService.createTopic(topicId, 1, "Topic de prueba", "it-test", null)

        val subscription = subscriptionService.createSubscription(
            topicId,
            SubscriptionRequest(
                topicId = topicId,
                topicVersion = 1,
                nameSub = "procesador-pull",
                description = "Sub pull de prueba",
                type = "PULL",
                urlRest = null,
                maxRetries = 10
            )
        )

        messageService.publish(topicId, 1, MessageRequest("jdoe", "reintegro.solicitado", 1, mapOf("monto" to 100)), topicToken = topic.publishToken)
        messageService.publish(topicId, 1, MessageRequest("jdoe", "reintegro.solicitado", 1, mapOf("monto" to 200)), topicToken = topic.publishToken)

        val firstPull = subscriptionService.pullMessages(subscription.id, 0, 10, subscription.token)
        assertEquals(2, firstPull.size)

        val firstMessage = firstPull.first() as Map<*, *>
        val firstHeader = firstMessage["header"] as Map<*, *>
        val firstMessageId = firstHeader["messageId"] as String
        subscriptionService.processAck(subscription.id, firstMessageId, subscription.token, "PROCESSED")

        Thread.sleep(1200)
        val secondPull = subscriptionService.pullMessages(subscription.id, 0, 10, subscription.token)
        assertEquals(1, secondPull.size)
    }

    @Test
    fun `pull hides leased messages for 10 seconds and returns next batch`() {
        val topicId = "reintegros-${UUID.randomUUID().toString().take(6)}"
        val topic = topicService.createTopic(topicId, 1, "Topic de prueba", "it-test", null)
        val subscription = subscriptionService.createSubscription(
            topicId,
            SubscriptionRequest(
                topicId = topicId,
                topicVersion = 1,
                nameSub = "procesador-batch",
                description = "Sub pull por lotes",
                type = "PULL",
                urlRest = null,
                maxRetries = 10
            )
        )

        (1..20).forEach { amount ->
            messageService.publish(topicId, 1, MessageRequest("jdoe", "reintegro.solicitado", 1, mapOf("monto" to amount)), topicToken = topic.publishToken)
        }

        val firstBatch = subscriptionService.pullMessages(subscription.id, 0, 10, subscription.token)
        val secondBatch = subscriptionService.pullMessages(subscription.id, 0, 10, subscription.token)
        val thirdBatchImmediate = subscriptionService.pullMessages(subscription.id, 0, 10, subscription.token)

        assertEquals(10, firstBatch.size)
        assertEquals(10, secondBatch.size)
        assertEquals(0, thirdBatchImmediate.size)

        Thread.sleep(11_000)
        val afterVisibilityTimeout = subscriptionService.pullMessages(subscription.id, 0, 10, subscription.token)
        assertEquals(10, afterVisibilityTimeout.size)
    }

    @Test
    fun `push dispatches one post to urlRest when webhook returns 200`() {
        val mock = MockWebServer()
        mock.start()
        try {
            mock.enqueue(MockResponse().setResponseCode(200).setBody("ok"))
            val topicId = "reintegros-${UUID.randomUUID().toString().take(6)}"
            val topic = topicService.createTopic(topicId, 1, "Topic de prueba", "it-test", null)
            val sub = subscriptionService.createSubscription(
                topicId,
                SubscriptionRequest(
                    topicId = topicId,
                    topicVersion = 1,
                    nameSub = "push-ok",
                    description = "PUSH it",
                    type = "PUSH",
                    urlRest = mock.url("/wh").toString(),
                    maxRetries = 10
                )
            )
            messageService.publish(topicId, 1, MessageRequest("jdoe", "push.demo", 1, mapOf("k" to 1)), topicToken = topic.publishToken)
            pushDispatchScheduler.dispatchPushMessages()
            assertNotNull(mock.takeRequest(5, TimeUnit.SECONDS))
            assertNull(rabbitTemplate.receive(sub.mainQueue, 500L))
        } finally {
            mock.shutdown()
        }
    }

    @Test
    fun `push moves message to dlq after max http failures`() {
        val mock = MockWebServer()
        mock.start()
        try {
            mock.enqueue(MockResponse().setResponseCode(503))
            mock.enqueue(MockResponse().setResponseCode(503))
            mock.enqueue(MockResponse().setResponseCode(503))
            val topicId = "reintegros-${UUID.randomUUID().toString().take(6)}"
            val topic = topicService.createTopic(topicId, 1, "Topic de prueba", "it-test", null)
            val sub = subscriptionService.createSubscription(
                topicId,
                SubscriptionRequest(
                    topicId = topicId,
                    topicVersion = 1,
                    nameSub = "push-fail",
                    description = "PUSH it",
                    type = "PUSH",
                    urlRest = mock.url("/wh").toString(),
                    maxRetries = 10
                )
            )
            messageService.publish(topicId, 1, MessageRequest("jdoe", "push.demo", 1, mapOf("k" to 1)), topicToken = topic.publishToken)
            pushDispatchScheduler.dispatchPushMessages()
            assertNotNull(rabbitTemplate.receive(sub.dlq, 5_000L))
            assertNull(rabbitTemplate.receive(sub.mainQueue, 500L))
        } finally {
            mock.shutdown()
        }
    }

    @Test
    fun `push sends message to dlq on 4xx without retries`() {
        val mock = MockWebServer()
        mock.start()
        try {
            mock.enqueue(MockResponse().setResponseCode(400).setBody("{\"error\":\"bad\"}"))
            val topicId = "reintegros-${UUID.randomUUID().toString().take(6)}"
            val topic = topicService.createTopic(topicId, 1, "Topic de prueba", "it-test", null)
            val sub = subscriptionService.createSubscription(
                topicId,
                SubscriptionRequest(
                    topicId = topicId,
                    topicVersion = 1,
                    nameSub = "push-400",
                    description = "PUSH it",
                    type = "PUSH",
                    urlRest = mock.url("/wh").toString(),
                    maxRetries = 10
                )
            )
            messageService.publish(topicId, 1, MessageRequest("jdoe", "push.demo", 1, mapOf("k" to 1)), topicToken = topic.publishToken)
            pushDispatchScheduler.dispatchPushMessages()
            assertNotNull(rabbitTemplate.receive(sub.dlq, 5_000L))
            assertNull(rabbitTemplate.receive(sub.mainQueue, 500L))
            assertNotNull(mock.takeRequest(3, TimeUnit.SECONDS))
            assertNull(mock.takeRequest(200, TimeUnit.MILLISECONDS))
        } finally {
            mock.shutdown()
        }
    }

    @Test
    fun `pull message moves automatically to dlq after reaching maxDeliveryCountPull`() {
        val topicId = "reintegros-${UUID.randomUUID().toString().take(6)}"
        val topic = topicService.createTopic(topicId, 1, "Topic de prueba", "it-test", null)
        val subscription = subscriptionService.createSubscription(
            topicId,
            SubscriptionRequest(
                topicId = topicId,
                topicVersion = 1,
                nameSub = "pull-max-delivery",
                description = "Sub pull con max delivery",
                type = "PULL",
                urlRest = null,
                maxRetries = 10,
                maxDeliveryCountPull = 2
            )
        )

        messageService.publish(
            topicId,
            1,
            MessageRequest("jdoe", "reintegro.solicitado", 1, mapOf("monto" to 150)),
            topicToken = topic.publishToken
        )

        assertEquals(1, subscriptionService.pullMessages(subscription.id, 0, 10, subscription.token).size)
        Thread.sleep(1200)
        assertEquals(1, subscriptionService.pullMessages(subscription.id, 0, 10, subscription.token).size)
        Thread.sleep(1200)
        assertEquals(0, subscriptionService.pullMessages(subscription.id, 0, 10, subscription.token).size)

        val dlqMessage = rabbitTemplate.receiveAndConvert(subscription.dlq, 5_000L)
        assertNotNull(dlqMessage)
        val dlqHeader = ((dlqMessage as Map<*, *>)["header"] as Map<*, *>)
        assertEquals("max-delivery-exceeded", dlqHeader["dlqReason"])
        assertTrue(rabbitTemplate.receiveAndConvert(subscription.mainQueue, 500L) == null)
    }

    companion object {
        @Container
        @JvmStatic
        val rabbit = RabbitMQContainer("rabbitmq:3.13-management")

        @Container
        @JvmStatic
        val mongo = MongoDBContainer("mongo:7")

        @JvmStatic
        @DynamicPropertySource
        fun configureProperties(registry: DynamicPropertyRegistry) {
            registry.add("spring.rabbitmq.host") { rabbit.host }
            registry.add("spring.rabbitmq.port") { rabbit.amqpPort }
            registry.add("spring.rabbitmq.username") { rabbit.adminUsername }
            registry.add("spring.rabbitmq.password") { rabbit.adminPassword }
            registry.add("spring.data.mongodb.uri") { mongo.replicaSetUrl }
        }
    }
}
