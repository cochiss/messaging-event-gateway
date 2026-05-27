package com.smg.meg.controller

import com.smg.meg.model.document.Topic
import com.smg.meg.service.TopicService
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
@RequestMapping("/topics")
@Tag(name = "Topics", description = "Administracion de topics versionados")
class TopicController(private val topicService: TopicService) {

    @GetMapping
    @Operation(
        summary = "Listar topics",
        description = "Retorna topics paginados. Sin `publishToken` (solo en POST create). Identificadores de canal con nombres agnosticos (`distributionChannel`)."
    )
    @ApiResponses(
        ApiResponse(
            responseCode = "200",
            description = "Listado de topics (sin publishToken)",
            content = [
                Content(
                    mediaType = "application/json",
                    examples = [
                        ExampleObject(
                            value = """[{"id":"topic-demo","version":1,"description":"Topic principal para publicar eventos de negocio","ownerApp":"app-demo","maxBodyBytes":65536,"distributionChannel":"ex.topic-demo","status":"ACTIVE"}]"""
                        )
                    ]
                )
            ]
        )
    )
    fun listTopics(
        @Parameter(description = "Numero de pagina (base 0)", example = "0")
        @RequestParam(defaultValue = "0") @Min(0) page: Int,
        @Parameter(description = "Tamano de pagina", example = "10")
        @RequestParam(defaultValue = "10") @Min(1) size: Int
    ): List<TopicResponse> =
        topicService.listTopics(page, size).map { TopicResponse.from(it) }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    @Operation(summary = "Crear topic", description = "Crea un topic con version secuencial y publishToken")
    @ApiResponses(
        ApiResponse(
            responseCode = "201",
            description = "Topic creado",
            content = [
                Content(
                    mediaType = "application/json",
                    examples = [
                        ExampleObject(
                            value = """{"id":"topic-demo","version":1,"description":"Topic principal para publicar eventos de negocio","ownerApp":"app-demo","publishToken":"4be8dbf7-f8f3-4a93-a45b-dffa029c9fd1","maxBodyBytes":65536,"distributionChannel":"ex.topic-demo","status":"ACTIVE"}"""
                        )
                    ]
                )
            ]
        ),
        ApiResponse(
            responseCode = "400",
            description = "Validacion o version invalida",
            content = [Content(mediaType = "application/json", examples = [ExampleObject(value = """{"error":"400 BAD_REQUEST","message":"Topic version must be >= 1"}""")])]
        ),
        ApiResponse(
            responseCode = "409",
            description = "Version repetida o menor a la actual",
            content = [Content(mediaType = "application/json", examples = [ExampleObject(value = """{"error":"409 CONFLICT","message":"Topic 'topic-demo' version 1 already exists or is older than current version 1"}""")])]
        )
    )
    fun createTopic(
        @SwaggerRequestBody(
            required = true,
            description = "Request para crear topic",
            content = [Content(mediaType = "application/json", schema = Schema(implementation = TopicRequest::class))]
        )
        @RequestBody @Valid request: TopicRequest
    ): TopicCreatedResponse = TopicCreatedResponse.from(
        topicService.createTopic(
            id = request.id,
            version = request.version,
            description = request.description,
            ownerApp = request.ownerApp,
            maxBodyBytes = request.maxBodyBytes
        )
    )

    @PutMapping("/{id}")
    @Operation(
        summary = "Actualizar topic",
        description = "Actualiza descripcion, ownerApp y maxBodyBytes. La respuesta NO incluye `publishToken` (solo se devuelve en el create)."
    )
    @ApiResponses(
        ApiResponse(
            responseCode = "200",
            description = "Topic actualizado (sin publishToken)",
            content = [
                Content(
                    mediaType = "application/json",
                    examples = [
                        ExampleObject(
                            value = """{"id":"topic-demo","version":1,"description":"Topic de eventos de negocio actualizado","ownerApp":"app-demo-v2","maxBodyBytes":131072,"distributionChannel":"ex.topic-demo","status":"ACTIVE"}"""
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
                            value = """{"error":"400 BAD_REQUEST","message":"maxBodyBytes must be >= 1"}"""
                        )
                    ]
                )
            ]
        ),
        ApiResponse(
            responseCode = "403",
            description = "Token de topic invalido",
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
                    examples = [
                        ExampleObject(
                            value = """{"error":"404 NOT_FOUND","message":"Topic 'topic-demo' not found"}"""
                        )
                    ]
                )
            ]
        )
    )
    fun updateTopic(
        @PathVariable
        @NotBlank
        @Pattern(
            regexp = "^[A-Za-z]+(-[A-Za-z]+)*$",
            message = "topic id must contain only letters and hyphens"
        )
        @Size(max = 50, message = "topic id must not exceed 50 characters")
        id: String,
        @RequestHeader("X-Topic-Token") @NotBlank topicToken: String,
        @RequestBody @Valid request: UpdateTopicRequest
    ): TopicResponse = TopicResponse.from(
        topicService.updateTopicConfig(
            id = id,
            description = request.description,
            ownerApp = request.ownerApp,
            maxBodyBytes = request.maxBodyBytes,
            topicToken = topicToken
        )
    )
}

data class TopicRequest(
    @field:NotBlank
    @field:Pattern(
        regexp = "^[A-Za-z]+(-[A-Za-z]+)*$",
        message = "id must contain only letters and hyphens"
    )
    @field:Size(max = 50, message = "id must not exceed 50 characters")
    @field:Schema(
        description = "Identificador de topic (kebab-case, max 50)",
        example = "topic-demo"
    )
    val id: String,
    @field:Min(1)
    @field:Schema(description = "Version secuencial del topic", example = "1")
    val version: Int,
    @field:NotBlank
    @field:Schema(description = "Descripcion funcional del topic", example = "Topic principal para publicar eventos de negocio")
    val description: String,
    @field:NotBlank
    @field:Schema(description = "Aplicacion propietaria del topic", example = "app-demo")
    val ownerApp: String,
    @field:Min(1)
    @field:Schema(description = "Maximo de bytes del payload", example = "65536")
    val maxBodyBytes: Int?
)

data class UpdateTopicRequest(
    @field:NotBlank
    @field:Schema(description = "Nueva descripcion", example = "Topic de eventos de negocio actualizado")
    val description: String,
    @field:NotBlank
    @field:Schema(description = "Nueva app owner", example = "app-demo-v2")
    val ownerApp: String,
    @field:Min(1)
    @field:Schema(description = "Nuevo maximo de bytes del payload", example = "131072")
    val maxBodyBytes: Int
)

data class TopicResponse(
    @field:Schema(example = "topic-demo")
    val id: String,
    @field:Schema(example = "1")
    val version: Int,
    val description: String?,
    @field:Schema(example = "app-demo")
    val ownerApp: String,
    @field:Schema(example = "65536")
    val maxBodyBytes: Int,
    @field:Schema(
        description = "Identificador del canal de distribucion del topic (nombre agnostico; valor operativo asignado por el gateway)",
        example = "ex.topic-demo"
    )
    val distributionChannel: String,
    @field:Schema(example = "ACTIVE")
    val status: String
) {
    companion object {
        fun from(topic: Topic): TopicResponse = TopicResponse(
            id = topic.id,
            version = topic.version,
            description = topic.description,
            ownerApp = topic.ownerApp,
            maxBodyBytes = topic.maxBodyBytes,
            distributionChannel = topic.rabbitExchange,
            status = topic.status
        )
    }
}

data class TopicCreatedResponse(
    @field:Schema(example = "topic-demo")
    val id: String,
    @field:Schema(example = "1")
    val version: Int,
    val description: String?,
    @field:Schema(example = "app-demo")
    val ownerApp: String,
    @field:Schema(description = "Token de publicacion; solo se devuelve al crear el topic", example = "4be8dbf7-f8f3-4a93-a45b-dffa029c9fd1")
    val publishToken: String,
    @field:Schema(example = "65536")
    val maxBodyBytes: Int,
    @field:Schema(description = "Canal de distribucion del topic", example = "ex.topic-demo")
    val distributionChannel: String,
    @field:Schema(example = "ACTIVE")
    val status: String
) {
    companion object {
        fun from(topic: Topic): TopicCreatedResponse = TopicCreatedResponse(
            id = topic.id,
            version = topic.version,
            description = topic.description,
            ownerApp = topic.ownerApp,
            publishToken = topic.publishToken,
            maxBodyBytes = topic.maxBodyBytes,
            distributionChannel = topic.rabbitExchange,
            status = topic.status
        )
    }
}