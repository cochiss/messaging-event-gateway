package com.smg.meg.controller

import com.smg.meg.service.MessageService
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.ExampleObject
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.parameters.RequestBody as SwaggerRequestBody
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.validation.Valid
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Pattern
import jakarta.validation.constraints.Size
import org.springframework.http.HttpStatus
import org.springframework.validation.annotation.Validated
import org.springframework.web.bind.annotation.*

@RestController
@Validated
@RequestMapping("/topics/{id}/v{version}/messages")
@Tag(name = "Messages", description = "Publicacion de mensajes en topics versionados")
class MessageController(private val messageService: MessageService) {

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    @Operation(summary = "Publicar mensaje", description = "Publica un mensaje y lo audita. Requiere headers de metadatos e idempotencia")
    @ApiResponses(
        ApiResponse(
            responseCode = "202",
            description = "Mensaje encolado",
            content = [
                Content(
                    mediaType = "application/json",
                    examples = [
                        ExampleObject(
                            name = "publishAccepted",
                            value = """{"messageId":"msg_950d117f","status":"ENQUEUED","idempotencyKey":"evt-001","deduplicated":"false"}"""
                        )
                    ]
                )
            ]
        ),
        ApiResponse(
            responseCode = "400",
            description = "Validacion invalida",
            content = [
                Content(
                    mediaType = "application/json",
                    examples = [
                        ExampleObject(
                            name = "missingHeader",
                            value = """{"error":"VALIDATION_ERROR","message":"Missing required header: Idempotency-Key"}"""
                        ),
                        ExampleObject(
                            name = "schemaValidationError",
                            value = """{"error":"400 BAD_REQUEST","message":"payload schema validation failed: $.cbu: expected minLength: 22, actual: 12"}"""
                        )
                    ]
                )
            ]
        ),
        ApiResponse(
            responseCode = "403",
            description = "X-Topic-Token invalido",
            content = [
                Content(
                    mediaType = "application/json",
                    examples = [
                        ExampleObject(
                            value = """{"error":"403 FORBIDDEN","message":"Invalid topic token for topic 'topic-demo'"}"""
                        )
                    ]
                )
            ]
        ),
        ApiResponse(
            responseCode = "404",
            description = "Topic no encontrado",
            content = [
                Content(
                    mediaType = "application/json",
                    examples = [ExampleObject(value = """{"error":"404 NOT_FOUND","message":"Topic 'topic-demo' not found"}""")]
                )
            ]
        ),
        ApiResponse(
            responseCode = "413",
            description = "Payload excede maxBodyBytes",
            content = [
                Content(
                    mediaType = "application/json",
                    examples = [
                        ExampleObject(
                            value = """{"error":"413 PAYLOAD_TOO_LARGE","message":"payload size 81234 exceeds maxBodyBytes 65536 for topic topic-demo"}"""
                        )
                    ]
                )
            ]
        )
    )
    fun publishMessage(
        @PathVariable
        @NotBlank
        @Pattern(
            regexp = "^[A-Za-z]+(-[A-Za-z]+)*$",
            message = "topic id must contain only letters and hyphens"
        )
        @Size(max = 50, message = "topic id must not exceed 50 characters")
        @Parameter(description = "ID de topic (kebab-case)", example = "topic-demo")
        id: String,
        @Parameter(description = "Version del topic", example = "1")
        @PathVariable version: Int,
        @Parameter(description = "Clave de idempotencia por topic/version", example = "evt-001")
        @RequestHeader("Idempotency-Key") @NotBlank idempotencyKey: String,
        @Parameter(description = "Token de publicacion del topic", example = "TOPIC_TOKEN_DEVUELTO_EN_CREATE_TOPIC")
        @RequestHeader("X-Topic-Token") @NotBlank topicToken: String,
        @Parameter(description = "Correlation ID transversal", example = "corr-001")
        @RequestHeader("X-Correlation-Id") @NotBlank correlationId: String,
        @Parameter(description = "Aplicacion origen", example = "finanzas-api")
        @RequestHeader("X-Source-App") @NotBlank sourceApp: String,
        @SwaggerRequestBody(
            required = true,
            description = "Envelope de publicacion de evento",
            content = [
                Content(
                    mediaType = "application/json",
                    examples = [
                        ExampleObject(
                            name = "publishRequest",
                            value = """{"user":"jdoe","eventType":"entidad.creada","eventVersion":1,"payload":{"amount":200,"document":"29345928","account":"0110567620056701234560"}}"""
                        )
                    ],
                    schema = Schema(implementation = MessageRequest::class)
                )
            ]
        )
        @RequestBody @Valid request: MessageRequest
    ) = messageService.publish(id, version, request, idempotencyKey, topicToken, correlationId, sourceApp)
}

data class MessageRequest(
    @field:NotBlank(message = "user must not be blank")
    @field:Schema(description = "Usuario funcional que dispara el evento", example = "jdoe")
    val user: String,       // Ejemplo: "jdoe" [cite: 37]
    @field:NotBlank(message = "eventType must not be blank")
    @field:Schema(description = "Tipo de evento de negocio", example = "entidad.creada")
    val eventType: String,
    @field:Min(value = 1, message = "eventVersion must be >= 1")
    @field:Schema(description = "Version del contrato de evento", example = "1", defaultValue = "1")
    val eventVersion: Int = 1,
    @field:Schema(
        description = "Payload libre validado por schema opcional del topic/version",
        example = """{"amount":200,"document":"29345928","account":"0110567620056701234560"}"""
    )
    val payload: Any        // El cuerpo genérico del mensaje [cite: 38]
)