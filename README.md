# Messaging Event Gateway

API de mensajería desacoplada: publicación a colas lógicas versionadas, suscripciones PULL/PUSH, entrega con visibilidad, ACK/REJECT, DLQ y requeue, sobre **RabbitMQ** y persistencia en **MongoDB**.

## Stack

- Java 17 · Kotlin · Spring Boot 3.2.4 · Maven
- RabbitMQ, MongoDB, Resilience4j
- Springdoc OpenAPI (Swagger)
- Spring Boot Actuator (health/metrics/prometheus)
- Bean Validation (`@Valid`, `jakarta.validation`) en requests de API (excepto publicación de mensaje)

## Requisitos

- Docker y Docker Compose (recomendado para Mongo + Rabbit + API)
- JDK 17 y Maven 3.9+ si corrés `mvn` en la máquina host (el proyecto trae `.mvn/` para forzar Maven Central si tu `~/.m2/settings.xml` apunta a un mirror inaccesible)

## Levantar con Docker Compose

> ⚠️ **Credenciales de demo local.** Tanto `application.yml` como `docker-compose.yml` usan
> usuario/clave `admin/admin123` para Mongo y RabbitMQ. Son **solo para el entorno de demo local**.
> En cualquier ambiente real (dev/qa/prod) reemplazá esos valores por variables de entorno
> gestionadas por un secret manager (Vault, AWS Secrets Manager, K8s Secrets, etc.) y nunca
> hagas commit de credenciales reales en el repositorio.

Desde esta carpeta:

```bash
docker compose up -d --build
```

Servicios: `app` (API), `rabbitmq`, `mongodb`.

- API: `http://localhost:8080`
- Swagger UI: `http://localhost:8080/swagger-ui/index.html`
- OpenAPI JSON: `http://localhost:8080/v3/api-docs`
- Actuator health: `http://localhost:8080/actuator/health`
- Actuator metrics: `http://localhost:8080/actuator/metrics`
- Actuator prometheus: `http://localhost:8080/actuator/prometheus`
- RabbitMQ Management: `http://localhost:15672` (usuario/clave según `docker-compose.yml`)

La documentación OpenAPI incluye ejemplos de request/response para flujo feliz y errores comunes (`400/401/403/404/409/413`) en endpoints principales de topics, messages, subscriptions y schema validation.

Si configurás una suscripción **PUSH** y el webhook corre **en tu máquina** (fuera de Docker), `urlRest` debe usar **`http://host.docker.internal:<puerto>/…`**, no `http://localhost:…`: el POST sale **desde el contenedor** del gateway y ahí `localhost` no es el host. El `docker-compose` incluye `extra_hosts` para resolver `host.docker.internal`.

### Reinicio parcial (solo MongoDB y RabbitMQ)

Si solo querés tocar infraestructura de mensajería/base de datos y **no** reconstruir la app:

```bash
# reinicio rapido, conserva datos
docker compose restart rabbitmq mongodb
```

```bash
# si estaban apagados, levantarlos sin tocar la app
docker compose up -d rabbitmq mongodb
```

Si querés **resetear solo MongoDB y RabbitMQ** (borra datos de ambos):

```bash
docker compose stop rabbitmq mongodb
docker compose rm -f rabbitmq mongodb
docker volume rm messaging-event-gateway_rabbitmq_data messaging-event-gateway_mongodb_data
docker compose up -d rabbitmq mongodb
```

Ver logs de la app del gateway:

```bash
docker compose logs -f app
```

Opcional (ultimas 200 lineas + follow):

```bash
docker compose logs -f --tail=200 app
```

## Ejecutar en local (sin contenedor de la app)

```bash
mvn spring-boot:run
```

Asegurate de tener MongoDB y RabbitMQ accesibles; por defecto `application.yml` apunta a `localhost` con las mismas credenciales que el compose.

## Contrato y reglas de negocio

Ver **[SPEC.md](SPEC.md)** (endpoints, visibilidad PULL, versionado de colas, índices Mongo, etc.).

Resumen:

- `topicId` y `nameSub`: solo letras + guion medio (`kebab-case`), maximo 50 caracteres, el gateway normaliza a lowercase.
- `Topic.version` obligatoria y secuencial por `id`.
- `Subscription.nameSub` única por topic.
- `Subscription.id` determinístico: `{topicId}-v{topicVersion}-{nameSub}`.
- PULL/ACK/DLQ: header `X-Sub-Token`.
- Visibilidad de mensajes PULL sin confirmar: configurable (`meg.pull.visibility-timeout-seconds`, por defecto 10 s).
- Límite de reentregas PULL por suscripción: `maxDeliveryCountPull` (default global `meg.pull.max-delivery-count-default=10`), editable por `PUT /subscriptions/{id}`.
- Si un mensaje PULL alcanza ese límite sin ACK/REJECT, el gateway lo mueve automáticamente a DLQ con `header.dlqReason=max-delivery-exceeded`.
- **PUSH:** POST a `urlRest`; **4xx** → **DLQ** al instante (sin reintento); **5xx / red** → reintentos con `meg.push.max-delivery-attempts` y luego **DLQ**. Detalle en [SPEC §3.4](SPEC.md#34-envio-push-webhook).
- PoC de resiliencia PUSH incluye métricas por suscripción (Micrometer `meg.push.deliveries.ok`, `meg.push.deliveries.failed`, `meg.push.retries`, `meg.push.dlq`, `meg.push.circuit.open`; en Prometheus suelen verse como `meg_push_deliveries_ok_total`, etc.) y logs estructurados `push_outcome` con `subscriptionId/messageId/correlationId/attempt/outcome`.
- `/actuator/prometheus` requiere la dependencia `micrometer-registry-prometheus` (incluida en el `pom.xml`); si ves `500` en ese endpoint, reconstruí la imagen o `mvn spring-boot:run` de nuevo.
- Los contadores `meg.push.*` (en scrape: `meg_push_*_total`) **solo aparecen después del primer incremento** (por ejemplo un dispatch PUSH que entregue, reprograme reintento o mande a DLQ). Si el scrape no los muestra aún, no hay tráfico PUSH que los haya tocado.

### Semántica de entrega (contrato formal)

- El gateway entrega mensajes con semántica **at-least-once** (PULL y PUSH).
- Esto implica que un mismo mensaje puede entregarse más de una vez (duplicados).
- No existe garantía de exactly-once end-to-end entre productor, gateway, Rabbit y consumidor.
- Todo consumidor debe implementar procesamiento **idempotente**.

### Idempotencia del consumidor (recomendación obligatoria)

Implementación sugerida:

1. Usar `messageId` o una clave de negocio estable como llave de deduplicación.
2. Antes de aplicar efectos de negocio (DB/API externa), verificar si esa llave ya fue procesada.
3. Si ya existe, responder éxito sin volver a ejecutar efectos.
4. Si no existe, ejecutar lógica y registrar la llave procesada idealmente en la misma transacción de negocio.

Para publish, el gateway ya exige `Idempotency-Key` y `X-Topic-Token` (token del topic) y deduplica por `topicId + topicVersion + idempotencyKey`.
Ademas, para metadatos transversales exige `X-Correlation-Id` y `X-Source-App`.

## Endpoints principales

- `POST /topics` · `GET /topics?page=0&size=10`
- `PUT /topics/{id}` (actualiza `description`, `ownerApp`, `maxBodyBytes`)
- `POST /topics/{id}/v{version}/messages` (headers obligatorios `Idempotency-Key`, `X-Topic-Token`, `X-Correlation-Id`, `X-Source-App`)
- `POST /topics/{id}/v{version}/schema-validation`
- `PUT /topics/{id}/v{version}/schema-validation`
- `GET /topics/{id}/v{version}/schema-validation`
- `POST /subscriptions` · `GET /subscriptions?page=0&size=10` (sin `token` en listados)
- `PUT /subscriptions/{id}` (actualiza `description`, `status`, `urlRest`, `maxDeliveryCountPull`)
- `GET /subscriptions/{id}/messages?page=0&size=10` (alias `rows` = `size`)
- `PUT /subscriptions/{id}/messages/{messageId}` (body `PROCESSED` / `REJECT`)
- `GET /subscriptions/{id}/dead-letters?page=0&size=10`
- `POST /subscriptions/{id}/dead-letters/{messageId}/requeue`

## Ejemplos curl (flujo completo)

### 1) Crear topic v1

```bash
curl -i -X POST "http://localhost:8080/topics" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "solicitudes-reintegros",
    "version": 1,
    "description": "Topic principal para publicar pedidos de reintegros",
    "ownerApp": "finanzas-api",
    "maxBodyBytes": 65536
  }'
```

`maxBodyBytes` es opcional en create; si no viene, se usa el default configurado en `application.yml` (`meg.topic.max-body-bytes`).
Al crear topic, el gateway devuelve `publishToken`; guardalo para usarlo en `X-Topic-Token` al publicar mensajes. El `GET /topics` NO devuelve `publishToken` (respuesta sanitizada). Si se pierde, el camino actual es recrear el topic en una nueva version.

### 2) Listar topics (paginado)

```bash
curl -i -X GET "http://localhost:8080/topics?page=0&size=10"
```

### 3) Crear subscription PULL

```bash
curl --location "http://localhost:8080/subscriptions" \
  -H "Content-Type: application/json" \
  -d '{
    "topicId": "solicitudes-reintegros",
    "topicVersion": 1,
    "nameSub": "pago-reintegros",
    "description": "procesador de reintegros por PULL",
    "type": "PULL",
    "urlRest": null,
    "maxRetries": 10,
    "maxDeliveryCountPull": 10
  }'
```

Guardar de la respuesta `id` y `token`:

```bash
SUB_ID="solicitudes-reintegros-v1-pago-reintegros"
SUB_TOKEN="TOKEN_DEVUELTO"
```

### 4) Publicar mensajes

Headers obligatorios en publish: `Idempotency-Key`, `X-Topic-Token`, `X-Correlation-Id`, `X-Source-App`.
Campos de contrato en body: `eventType` (obligatorio), `eventVersion` (opcional, default `1`).
Ademas, el payload se valida contra `maxBodyBytes` del topic. Si excede, responde `413 Payload Too Large`.

```bash
curl -i -X POST "http://localhost:8080/topics/solicitudes-reintegros/v1/messages" \
  -H "Idempotency-Key: evt-001" \
  -H "X-Topic-Token: TOPIC_TOKEN_DEVUELTO_EN_CREATE_TOPIC" \
  -H "X-Correlation-Id: corr-001" \
  -H "X-Source-App: finanzas-api" \
  -H "Content-Type: application/json" \
  -d '{
    "user": "jdoe",
    "eventType": "reintegro.solicitado",
    "eventVersion": 1,
    "payload": {"monto": 100}
  }'
```

### 4.1) Configurar schema validation por topic/version

Si un topic/version **no** tiene schema configurado, el publish **no valida** schema.
Si tiene schema configurado y `enabled=true`, el gateway valida `payload` antes de publicar.

Ejemplo (caso `monto`, `cbu`, `du`):

```bash
curl -i -X POST "http://localhost:8080/topics/solicitudes-reintegros/v1/schema-validation" \
  -H "Content-Type: application/json" \
  -d '{
    "enabled": true,
    "description": "Schema solicitudes-reintegros v1",
    "schema": {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "required": ["monto", "cbu", "du"],
      "properties": {
        "monto": {
          "type": "number",
          "exclusiveMinimum": 0
        },
        "cbu": {
          "type": "string",
          "minLength": 22,
          "maxLength": 22
        },
        "du": {
          "type": "string",
          "minLength": 7,
          "maxLength": 8,
          "pattern": "^[0-9]+$"
        }
      },
      "additionalProperties": true
    }
  }'
```

Actualizar schema:

```bash
curl -i -X PUT "http://localhost:8080/topics/solicitudes-reintegros/v1/schema-validation" \
  -H "Content-Type: application/json" \
  -d '{ ... }'
```

Consultar schema:

```bash
curl -i -X GET "http://localhost:8080/topics/solicitudes-reintegros/v1/schema-validation"
```

Payload ejemplo válido para ese schema:

```json
{
  "monto": 250,
  "cbu": "2312312312331234567890",
  "du": "29345928"
}
```

Payload ejemplo **inválido** (responde `400`, porque `cbu` no tiene length 22):

```json
{
  "monto": 250,
  "cbu": "231231231233",
  "du": "29345928"
}
```

Si el payload no cumple schema, responde `400` con detalle de validación y no publica en Rabbit.

```bash
curl --location 'http://localhost:8080/topics/solicitudes-reintegros/v1/messages' \
--header 'Idempotency-Key: evt-002' \
--header 'X-Topic-Token: TOPIC_TOKEN_DEVUELTO_EN_CREATE_TOPIC' \
--header 'X-Correlation-Id: corr-002' \
--header 'X-Source-App: finanzas-api' \
--header 'Content-Type: application/json' \
--data '{
    "user": "jdoe",
    "eventType": "reintegro.solicitado",
    "payload": {
        "monto": 200,
        "du": "29345928",
        "cbu": "0110567620056701234560"
    }
}'
```

### 5) Listar subscriptions

```bash
curl -i -X GET "http://localhost:8080/subscriptions?page=0&size=10"
```

```bash
curl -i -X GET "http://localhost:8080/subscriptions?topicId=solicitudes-reintegros&page=0&size=10"
```

### 6) Pull de mensajes

```bash
curl -i -X GET "http://localhost:8080/subscriptions/$SUB_ID/messages?page=0&size=10" \
  -H "X-Sub-Token: $SUB_TOKEN"
```

Alias de tamaño:

```bash
curl -i -X GET "http://localhost:8080/subscriptions/$SUB_ID/messages?rows=10" \
  -H "X-Sub-Token: $SUB_TOKEN"
```

Nota de comportamiento PULL:
- si el cliente no hace ACK/REJECT, cada nueva entrega incrementa `deliveryCount`;
- cuando `deliveryCount` alcanza `maxDeliveryCountPull`, el mensaje ya no se vuelve a entregar por PULL y pasa automáticamente a DLQ con motivo `max-delivery-exceeded`.

### 7) ACK / REJECT por mensaje

```bash
MESSAGE_ID="msg_xxxxxxxx"
curl -i -X PUT "http://localhost:8080/subscriptions/$SUB_ID/messages/$MESSAGE_ID" \
  -H "Content-Type: application/json" \
  -H "X-Sub-Token: $SUB_TOKEN" \
  -d '{"action":"PROCESSED"}'
```

```bash
curl -i -X PUT "http://localhost:8080/subscriptions/$SUB_ID/messages/$MESSAGE_ID" \
  -H "Content-Type: application/json" \
  -H "X-Sub-Token: $SUB_TOKEN" \
  -d '{"action":"REJECT"}'
```

### 8) DLQ y requeue

```bash
curl -i -X GET "http://localhost:8080/subscriptions/$SUB_ID/dead-letters?page=0&size=10" \
  -H "X-Sub-Token: $SUB_TOKEN"
```

```bash
curl -i -X POST "http://localhost:8080/subscriptions/$SUB_ID/dead-letters/$MESSAGE_ID/requeue" \
  -H "X-Sub-Token: $SUB_TOKEN"
```

### 9) Actualizar configuración del topic

Permite cambiar solo `description`, `ownerApp` y `maxBodyBytes`.

```bash
curl -i -X PUT "http://localhost:8080/topics/solicitudes-reintegros" \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Topic de solicitudes de reintegros actualizado",
    "ownerApp": "finanzas-api-v2",
    "maxBodyBytes": 131072
  }'
```

## Tests

Unitarios: `TopicServiceTest`, `SubscriptionServiceTest`.

Integración (Testcontainers): `PullAckFlowIntegrationTest` (PULL/ACK, visibilidad, **PUSH** 200, PUSH 5xx→DLQ, PUSH 4xx→DLQ sin reintento).

```bash
mvn clean test
```

## Pendientes / roadmap

Mejoras posibles sobre el estado actual del gateway (no implementadas aún):

- **Consistencia auditoría ↔ publicación (Outbox)**  
  - **Falta:** hoy auditoría y publicación no están coordinadas transaccionalmente.  
  - **Cómo resolver:** implementar patrón Outbox (persistir evento en Mongo y publicarlo asíncronamente con worker/reintentos/reconciliación).

- **Rate limit por topic/tenant**  
  - **Falta:** control de saturación de publish por ventana temporal.  
  - **Cómo resolver:** configuración por topic (`rateLimitCount`, `rateLimitWindow`, `rateLimitUnit`) y respuesta `429 Too Many Requests` al exceder.

- **PUSH resiliente (nivel producción)**  
  - **Estado actual (PoC):** existe resiliencia por suscripción con `circuit breaker`, `timeout`, `bulkhead`, `retry` + `conf-resilience`, métricas base y logs estructurados.
  - **Falta para producción:**
    - dashboards y alertas operables (SLO/SLA) sobre métricas de resiliencia;
    - trazabilidad distribuida end-to-end (propagación de trace/contexto hacia webhook);
    - gobernanza de cambios en `conf-resilience` (auditoría y rollback operativo);
    - estrategia robusta multi-instancia para cache de config/invalidez cruzada;
    - campañas de stress/soak/chaos con criterios formales de aceptación.

- **Observabilidad operable**  
  - **Falta:** integración real con **Dynatrace** y trazabilidad end-to-end.  
  - **Cómo resolver:** instrumentar trazas/métricas/logs con correlation ID, dashboards y alertas (latencia, errores, DLQ, retries).

- **Gobierno de datos de auditoría (MongoDB)**  
  - **Estado actual:** retención online resuelta con TTL de 30 días en `messages_audit` (`spring.data.mongodb.ttl=2592000`).  
  - **Pendiente opcional:** si se requiere histórico por topic más allá de 30 días, implementar estrategia de archivado/consulta histórica por topic.

- **Seguridad del perímetro**  
  - **Falta:** hoy se asume protección externa por API Manager.  
  - **Cómo resolver:** mantener API Manager como control principal y evaluar mTLS interno y hardening adicional según criticidad.
