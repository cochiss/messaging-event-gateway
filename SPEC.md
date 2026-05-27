# Messaging Event Gateway · SPEC

## Indice

1. [Objetivo](#1-objetivo)
2. [Stack](#2-stack)
3. [Modelo funcional](#3-modelo-funcional)
   - 3.1 [Semantica de entrega](#31-semantica-de-entrega-contrato)
   - 3.2 [Publicacion](#32-publicacion)
   - 3.3 [Consumo PULL](#33-consumo-pull)
   - 3.4 [DLQ y reproceso](#34-dlq-y-reproceso)
   - 3.5 [Envio PUSH (webhook)](#35-envio-push-webhook)
4. [Contratos API](#4-contratos-api)
   - 4.1 [Topics](#41-topics)
   - 4.2 [Messages](#42-messages)
   - 4.3 [Subscriptions](#43-subscriptions)
   - 4.4 [Pull / Ack / DLQ](#44-pull--ack--dlq)
   - 4.5 [Configuracion de resiliencia PUSH](#45-configuracion-de-resiliencia-push-conf-resilience)
5. [Reglas](#5-reglas)
   - 5.1 [Reglas de negocio](#51-reglas-de-negocio)
   - 5.2 [Buenas practicas API](#52-buenas-practicas-api)
6. [Persistencia](#6-persistencia)
7. [Estado actual de implementacion](#7-estado-actual-de-implementacion)
8. [Testing](#8-testing)
9. [Operacion y configuracion](#9-operacion-y-configuracion)
10. [Seguridad y observabilidad](#10-seguridad-y-observabilidad)
11. [Roadmap / Pendientes](#11-roadmap--pendientes)

---

## 1. Objetivo

Proveer una API de mensajeria desacoplada sobre RabbitMQ con:
- topics logicos versionados,
- suscripciones por consumidor,
- modo PULL/PUSH,
- auditoria y control en MongoDB.

### 1.1 Guia ARQEVENT (integradores)

Guia transversal para equipos que integran con MEG: [`docs/arqevent/README.md`](docs/arqevent/README.md) (patrones PUSH/PULL, trazabilidad, `pull-consumer-lib`, DLQ y operacion).

## 2. Stack

- Kotlin + Spring Boot 3.2.4 (Java 17)
- RabbitMQ (topic exchange + queue por subscription)
- MongoDB
- Resilience4j (PUSH)
- Springdoc OpenAPI (Swagger)
- Spring Boot Actuator (health, metrics, prometheus)
- Bean Validation (`jakarta.validation`) en contratos API

## 3. Modelo funcional

### 3.1 Semantica de entrega (contrato)

- Garantia del gateway: **at-least-once** para PULL y PUSH.
- Pueden existir duplicados por reintentos, redelivery y fallos transitorios.
- Los consumidores deben ser idempotentes; no se garantiza exactly-once end-to-end.

### 3.2 Publicacion

1. Productor publica en `POST /topics/{id}/v{version}/messages`.
2. Se audita en Mongo (`messages_audit`).
3. Se publica en el exchange `ex.{topicId}`.
4. Rabbit distribuye a las colas de suscripciones (internas de infraestructura).

### 3.3 Consumo PULL

1. Cliente consulta `GET /subscriptions/{id}/messages`.
2. El servicio entrega mensajes pendientes y/o consume de Rabbit.
3. Los mensajes entregados quedan invisibles por `visibility-timeout-seconds` si no se confirma.
4. Cliente confirma con `PUT .../messages/{messageId}` (`PROCESSED` o `REJECT`).

#### 3.3.1 Visibility timeout

- Al entregar un mensaje por PULL, se persiste/actualiza en `subscription_pending_messages`:
  - `visibilityUntil`: timestamp hasta el cual el mensaje permanece invisible.
  - `deliveryCount`: contador de reentregas.
- Regla de visibilidad:
  - Si `visibilityUntil` es `null` o ya vencio, el mensaje es visible.
  - Si `visibilityUntil` esta en el futuro, no se entrega en nuevos pulls.
- No se usa cron para "desinvisibilizar": la visibilidad se resuelve por lectura (lazy evaluation) comparando `visibilityUntil` con `now`.
- Confirmacion:
  - `PROCESSED`: elimina el pending.
  - `REJECT`: mueve a DLQ y elimina el pending de MAIN.
- Si no se confirma, el mensaje vuelve a estar disponible al vencer `visibilityUntil`.

#### 3.3.2 Poison message handling

- Si `deliveryCount` alcanza `maxDeliveryCountPull` de la suscripcion, el mensaje se mueve automaticamente a DLQ con motivo `max-delivery-exceeded` y deja de entregarse por PULL.
- Default global: `meg.pull.max-delivery-count-default` (default `10`).
- Editable por suscripcion via `PUT /subscriptions/{id}` (campo `maxDeliveryCountPull`).

### 3.4 DLQ y reproceso

- `REJECT` mueve un mensaje de MAIN a DLQ.
- `GET /subscriptions/{id}/dead-letters` lista las dead letters.
- `POST .../dead-letters/{messageId}/requeue` reencola a MAIN.

### 3.5 Envio PUSH (webhook)

- Un scheduler despacha mensajes de la cola MAIN de cada suscripcion **PUSH** hacia `urlRest` (POST con el cuerpo del mensaje).
- Semantica por respuesta HTTP:
  - **2xx**: entregado (no va a DLQ).
  - **4xx**: error definitivo del destino (payload invalido, auth, etc.) → DLQ **inmediato**, sin reintentos.
  - **5xx / 3xx no exitosa / fallo de red (timeout, connection refused, etc.)**: reintentos hasta `meg.push.max-delivery-attempts` (o `maxRetries` de la suscripcion si la propiedad es `0`); agotados los intentos → DLQ.
- Cabecera de control: `x-meg-push-attempt` (contador de intentos).
- Intervalo entre ciclos de despacho: `meg.push.dispatch-interval-ms` (default `2000` ms).

#### 3.5.1 Resiliencia PUSH por suscripcion (`conf-resilience`)

- Cada suscripcion PUSH tiene una policy persistida en `conf_resilience` + cache en memoria al arrancar.
- Path de administracion: `.../conf-resilience` (ver §4.5).
- Al crear una suscripcion `type=PUSH`, se crea automaticamente el documento con defaults globales.
- Estados de la suscripcion (ver §5.1) reflejan la salud tecnica:
  - `PAUSED_BY_SYSTEM` es un estado tecnico temporal manejado por el sistema, **no** se setea manualmente por API.
- Politica recomendada de ejecucion (orden):
  1. `bulkhead` (limite de concurrencia por suscripcion).
  2. `timeout` de llamada HTTP.
  3. `retry` con backoff+jitter para errores transitorios (5xx/red/timeout).
  4. `circuit-breaker` para proteger destino y gateway ante degradacion sostenida.
- Semantica resumida:
  - 2xx: `delivered`.
  - 4xx: DLQ inmediato, sin retry.
  - 5xx/red/timeout: retry segun policy; al agotar, DLQ.
  - circuito abierto: se evita intento real al webhook mientras dure la ventana OPEN.

#### 3.5.2 Alcance PoC de resiliencia PUSH

- Obligatorio en la PoC:
  - policies por suscripcion (`retry`, `timeout`, `bulkhead`, `circuit-breaker`);
  - metricas minimas por suscripcion (IDs Micrometer `meg.push.*`; en scrape Prometheus aparecen como `meg_push_*_total`):
    - `meg.push.deliveries.ok` → `meg_push_deliveries_ok_total`
    - `meg.push.deliveries.failed` → `meg_push_deliveries_failed_total`
    - `meg.push.retries` → `meg_push_retries_total`
    - `meg.push.dlq` → `meg_push_dlq_total` (tag `outcome` cuando aplica)
    - `meg.push.circuit.open` → `meg_push_circuit_open_total`
  - los contadores se registran de forma perezosa: hasta el primer evento que los incremente, pueden no aparecer en `/actuator/prometheus`.
  - logs estructurados por intento/resultado con `subscriptionId`, `messageId`, `correlationId`, `attempt`, `outcome`.
- Outcomes esperados: `delivered`, `retry_scheduled`, `dlq`, `4xx_direct_dlq`, `retry_exhausted_dlq`.
- Criterio de aceptacion PoC: con webhook degradado, el gateway no cae, los mensajes siguen una politica deterministica (retry o DLQ segun tipo de error) y la suscripcion puede transicionar a `PAUSED_BY_SYSTEM`.

## 4. Contratos API

### 4.1 Topics

- `POST /topics`
  - Request:
```json
{
  "id": "solicitudes-reintegros",
  "version": 1,
  "description": "Topico principal para publicar pedidos de reintegros",
  "ownerApp": "finanzas-api",
  "maxBodyBytes": 65536
}
```
  - `maxBodyBytes` es opcional; si no se envia, usa `meg.topic.max-body-bytes`.
  - Respuesta incluye `publishToken`. Solo se devuelve aqui (al momento de la creacion). Guardarlo: no es consultable luego.
  - Incluye `distributionChannel` (nombre agnostico del canal de distribucion); ver ley §5.2.

- `GET /topics?page=0&size=10`
  - Respuesta sanitizada: **no** incluye `publishToken`; **si** incluye `distributionChannel`.

- `PUT /topics/{id}`
  - Header obligatorio: `X-Topic-Token` (debe coincidir con el `publishToken` del topic; si no, `403`).
  - Permite actualizar solo `description`, `ownerApp`, `maxBodyBytes`.
  - Respuesta sanitizada: **no** incluye `publishToken`; **si** incluye `distributionChannel` (igual que `GET /topics`).
```json
{
  "description": "Topic de solicitudes de reintegros actualizado",
  "ownerApp": "finanzas-api-v2",
  "maxBodyBytes": 131072
}
```

- `POST /topics/{id}/v{version}/schema-validation`
  - Header obligatorio: `X-Topic-Token` (si no coincide, `403`).
  - Crea schema validation para un topic/version.
  - Si no existe schema para un topic/version, el publish no valida schema.
- `PUT /topics/{id}/v{version}/schema-validation`
  - Header obligatorio: `X-Topic-Token` (si no coincide, `403`).
  - Actualiza schema existente.
- `GET /topics/{id}/v{version}/schema-validation` · obtiene schema configurado (sin token).
- Request de create/update:
```json
{
  "enabled": true,
  "description": "Schema solicitudes-reintegros v1",
  "schema": {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["monto", "cbu", "du"],
    "properties": {
      "monto": { "type": "number", "exclusiveMinimum": 0 },
      "cbu": { "type": "string", "minLength": 22, "maxLength": 22 },
      "du": { "type": "string", "minLength": 7, "maxLength": 8, "pattern": "^[0-9]+$" }
    },
    "additionalProperties": true
  }
}
```

### 4.2 Messages

- `POST /topics/{id}/v{version}/messages`
  - Headers obligatorios: `Idempotency-Key`, `X-Topic-Token`, `X-Correlation-Id`, `X-Source-App`.
  - `X-Topic-Token` debe coincidir con el token del topic; si no, `403`.
  - Body: `eventType` obligatorio, `eventVersion` opcional (default `1`).
  - Valida tamaño de `payload` contra `topic.maxBodyBytes`. Si excede:
    - `413 Payload Too Large` con mensaje claro: `payload size 81234 exceeds maxBodyBytes 65536 for topic solicitudes-reintegros`.
  - Si hay schema validation habilitado, valida `payload` contra JSON Schema (draft-07).

### 4.3 Subscriptions

- `POST /subscriptions`
  - Request:
```json
{
  "topicId": "solicitudes-reintegros",
  "topicVersion": 2,
  "nameSub": "procesador-v2",
  "description": "Sub PULL",
  "type": "PULL",
  "urlRest": null,
  "maxRetries": 10,
  "maxDeliveryCountPull": 10
}
```
  - Response relevante (canales con nombres agnosticos):
```json
{
  "id": "solicitudes-reintegros-v2-procesador-v2",
  "token": "uuid-token",
  "deliveryChannel": "q.solicitudes-reintegros.v2.procesador-v2",
  "deadLetterChannel": "q.solicitudes-reintegros.v2.procesador-v2.dlq",
  "topicId": "solicitudes-reintegros",
  "topicVersion": 2,
  "nameSub": "procesador-v2",
  "type": "PULL",
  "status": "ACTIVE"
}
```
- `GET /subscriptions?page=0&size=10`
  - Respuesta sanitizada: **no** incluye `token`; **si** incluye `deliveryChannel` y `deadLetterChannel`.
  - Filtro opcional: `topicId`.
- `PUT /subscriptions/{id}`
  - Header obligatorio: `X-Sub-Token` (si no coincide, `401`).
  - Permite actualizar `description`, `status` (`ACTIVE` / `INACTIVE`), `urlRest` (solo PUSH; en PULL debe ser nulo), `maxDeliveryCountPull`.
  - `PAUSED_BY_SYSTEM` es estado tecnico y no se setea por este endpoint.

### 4.4 Pull / Ack / DLQ

- `GET /subscriptions/{id}/messages?page=0&size=10`
  - `rows` puede usarse como alias de `size`.
- `PUT /subscriptions/{id}/messages/{messageId}`
```json
{"action":"PROCESSED"}
```
o
```json
{"action":"REJECT"}
```
- `GET /subscriptions/{id}/dead-letters?page=0&size=10`
- `POST /subscriptions/{id}/dead-letters/{messageId}/requeue`

### 4.5 Configuracion de resiliencia PUSH (`conf-resilience`)

- `GET /subscriptions/{id}/conf-resilience` · obtiene policy persistida (sin token).
- `PUT /subscriptions/{id}/conf-resilience`
  - Header obligatorio: `X-Sub-Token` (si no coincide, `401`).
  - Actualiza policy.
- `DELETE /subscriptions/{id}/conf-resilience`
  - Header obligatorio: `X-Sub-Token` (si no coincide, `401`).
  - Resetea a defaults (no elimina la fila).

Reglas:
- Aplica solo a suscripciones `type=PUSH`.
- Si la suscripcion no existe: `404`.
- Si la suscripcion no es PUSH: `400`.

Defaults iniciales al crear suscripcion PUSH:

- `enabled=true`
- `timeoutMs=2000`
- `retry.maxAttempts=3`
- `retry.initialBackoffMs=500`
- `retry.multiplier=2.0`
- `retry.maxBackoffMs=5000`
- `retry.jitterFactor=0.2`
- `circuitBreaker.failureRateThreshold=50`
- `circuitBreaker.slowCallRateThreshold=70`
- `circuitBreaker.slowCallDurationMs=2000`
- `circuitBreaker.minimumNumberOfCalls=10`
- `circuitBreaker.slidingWindowSize=20`
- `circuitBreaker.waitDurationInOpenStateMs=30000`
- `circuitBreaker.permittedCallsInHalfOpenState=3`
- `bulkhead.maxConcurrentCalls=10`
- `bulkhead.maxWaitDurationMs=0`

Validaciones minimas:

- `timeoutMs > 0`
- `retry.maxAttempts >= 1`
- `retry.multiplier >= 1.0`
- `circuitBreaker.failureRateThreshold` y `slowCallRateThreshold` en `1..100`
- `circuitBreaker.minimumNumberOfCalls <= slidingWindowSize`
- `bulkhead.maxConcurrentCalls >= 1`

### 4.6 Headers (resumen)

- `X-Sub-Token` · obligatorio en pull/ack/dlq, `PUT /subscriptions/{id}`, `PUT/DELETE .../conf-resilience`.
- `Idempotency-Key` · obligatorio en publish.
- `X-Topic-Token` · obligatorio en publish, `PUT /topics/{id}`, `POST/PUT .../schema-validation`.
- `X-Correlation-Id` · obligatorio en publish.
- `X-Source-App` · obligatorio en publish.

## 5. Reglas

### 5.1 Reglas de negocio

- Estandar de nombres para `topicId` y `nameSub`:
  - solo letras y guion medio (kebab-case): `^[a-z]+(-[a-z]+)*$`,
  - maximo 50 caracteres,
  - no se permiten espacios, underscores, puntos ni numeros,
  - el gateway normaliza automaticamente a lowercase antes de persistir/operar.
- `Topic.version` obligatoria.
- Versionado secuencial por topic:
  - primera alta: `version = 1`,
  - siguiente alta: `current + 1`,
  - repetida o salteada: error.
- `Subscription.nameSub` unica por `topicId`.
- `Subscription.id` deterministico: `{topicId}-v{topicVersion}-{nameSub}`.
- `Subscription.status`:
  - `ACTIVE`: habilitada para operar.
  - `INACTIVE`: administrativamente deshabilitada.
  - `PAUSED_BY_SYSTEM`: pausa tecnica temporal gestionada por resiliencia PUSH.
- ACK es por mensaje y por subscription.
  - `PROCESSED` elimina solo ese pending.
  - `REJECT` mueve solo ese pending a DLQ.
- PULL:
  - Mensaje leido sin ACK/REJECT permanece invisible por `meg.pull.visibility-timeout-seconds` (default `10` s).
  - `maxDeliveryCountPull` define cuantas entregas se permiten antes de DLQ automatico.
  - Default global: `meg.pull.max-delivery-count-default` (default `10`).
- Publish:
  - `Idempotency-Key` repetido para el mismo topic/version: el gateway evita duplicados y responde con el `messageId` ya registrado.
  - `X-Topic-Token` debe coincidir con el token configurado del topic.
  - `eventType` obligatorio; `eventVersion` opcional (default `1`).
  - `payload` no puede exceder `topic.maxBodyBytes`.
  - Si hay schema validation habilitado y no cumple, responde `400` y no publica.
- Todo consumidor debe implementar idempotencia para manejar potenciales duplicados por semantica at-least-once.
- Estrategia recomendada de consumidor: deduplicar por `messageId` o clave de negocio estable antes de ejecutar efectos.

### 5.2 Buenas practicas API

#### Agnosticismo tecnologico en contratos API (ley)

Los recursos expuestos por la API REST del gateway son **contratos de negocio y operacion**. Los identificadores de canales/colas **si se exponen** cuando sirven para operacion o referencia (guardar en Postman, troubleshooting), pero los **nombres de atributos** no deben acoplarse a una tecnologia concreta.

- **Ley:** ningun nombre de atributo en request/response de la API publica debe revelar el producto o broker de implementacion (ej. prohibido `rabbitExchange`, `rabbitQueue`, `kafkaTopic`, etc.).
- **Nombres agnosticos en API** (mapeo desde persistencia interna):

| Atributo API | Campo interno (Mongo / servicio) | Significado |
|--------------|-----------------------------------|-------------|
| `distributionChannel` | `rabbitExchange` | Canal donde el gateway publica/distribuye mensajes del topic |
| `deliveryChannel` | `mainQueue` | Canal principal de entrega de la suscripcion |
| `deadLetterChannel` | `dlq` | Canal de mensajes no procesables (dead letter) |

- Los **valores** pueden conservar la convencion operativa actual (`ex.{topicId}`, `q.{topic}-v{n}-{nameSub}`, etc.); la ley aplica al **nombre del campo JSON**, no al formato del identificador.
- **Sanitizacion de secretos:** `publishToken` y `token` de suscripcion solo en la respuesta del `POST` de alta; listados y updates no los exponen (ver §4.1 y §4.3).

- Todo endpoint `GET` implementa paginado obligatorio con `page` y `size`.
  - `page` es base 0.
  - `size` debe tener limite razonable definido por implementacion para evitar lecturas masivas.
- Validacion de contratos en capa controller con `@Valid`/constraints para requests de Topic/Subscription/Ack.
- Excepcion explicita: `POST /topics/{id}/v{version}/messages` mantiene `payload` libre por defecto, pero puede validarse con schema opcional por topic/version.
- Errores API deben responder mensajes claros y accionables (evitar `Bad Request` generico sin detalle).
- Formato de error estandarizado:
```json
{
  "error": "VALIDATION_ERROR",
  "message": "Missing required header: Idempotency-Key"
}
```
- Casos esperados:
  - Header faltante `Idempotency-Key` → `400` + mensaje explicito.
  - Header vacio `Idempotency-Key` → `400` + mensaje explicito.
  - Header faltante `X-Topic-Token`, `X-Correlation-Id` o `X-Source-App` (publish) → `400` + mensaje explicito.
  - Header faltante `X-Topic-Token` (`PUT /topics/{id}`, `POST/PUT .../schema-validation`) → `400` + mensaje explicito.
  - Header faltante `X-Sub-Token` (`PUT /subscriptions/{id}`, pull/ack/dlq, `PUT/DELETE .../conf-resilience`) → `400` + mensaje explicito.
  - `X-Topic-Token` invalido (publish, actualizar topic, schema write) → `403` + mensaje explicito (`Invalid topic token for topic '...'`).
  - `X-Sub-Token` invalido o id/token no coinciden (consumo PULL, actualizar sub, conf-resilience write) → `401` + mensaje explicito (`Invalid subscription token`).
  - `eventType` vacio → `400` + mensaje explicito.
  - `eventVersion < 1` → `400` + mensaje explicito.
  - Fallos de validacion de body/params → `400` + detalle del campo/regla.

## 6. Persistencia

Colecciones Mongo:
- `topics`
- `subscriptions`
- `messages_audit`
- `subscription_pending_messages` (estado PULL/DLQ persistente)
- `topic_schema_validation`
- `conf_resilience` (policy de resiliencia por suscripcion PUSH)

Indices Mongo relevantes:
- `messages_audit`:
  - TTL sobre `header.createdAt` (configurable por `spring.data.mongodb.ttl`, default `2592000` = 30 dias).
  - Indice unico `(topicId, topicVersion, idempotencyKey)` para deduplicacion fuerte en concurrencia (`ux_messages_audit_topic_version_idempotency`).
- `subscription_pending_messages`:
  - `idx_pending_sub_bucket_created` sobre (`subscriptionId`, `bucket`, `createdAt`) para ordenar lectura de pendientes.
  - `idx_pending_sub_bucket_visibility` sobre (`subscriptionId`, `bucket`, `visibilityUntil`) para filtrar visibles/invisibles eficientemente.

## 7. Estado actual de implementacion

Implementado:
- Topics
  - alta con versionado secuencial obligatorio y validaciones.
  - actualizacion controlada (`description`, `ownerApp`, `maxBodyBytes`) con `X-Topic-Token` obligatorio.
  - `publishToken` retornado **solo** en create, no en listados; canal expuesto como `distributionChannel` (ley §5.2).
  - schema validation opcional por `topicId`/`version` (JSON Schema draft-07); alta/actualizacion de schema exigen `X-Topic-Token`; lectura (`GET`) sin token.
- Subscriptions
  - alta unica por `topicId`+`nameSub` con `id` deterministico.
  - `status`: `ACTIVE` / `INACTIVE` / `PAUSED_BY_SYSTEM` (tecnico, manejado por sistema).
  - `token` retornado **solo** en create, no en listados.
  - actualizacion controlada (`description`, `status`, `urlRest`, `maxDeliveryCountPull`) con `X-Sub-Token` obligatorio.
- Mensajes (publish)
  - headers obligatorios `Idempotency-Key`, `X-Topic-Token`, `X-Correlation-Id`, `X-Source-App`.
  - validacion de tamaño (`maxBodyBytes`) y schema opcional.
  - auditoria en `messages_audit` con TTL.
  - deduplicacion fuerte por indice unico `(topicId, topicVersion, idempotencyKey)`.
- PULL
  - lectura paginada con `X-Sub-Token`.
  - visibility timeout configurable, lazy evaluation.
  - ACK `PROCESSED` / `REJECT`.
  - DLQ + requeue.
  - **poison message handling**: auto-DLQ al alcanzar `maxDeliveryCountPull`.
- PUSH
  - scheduler con dispatch periodico.
  - manejo diferenciado 2xx/4xx/5xx-red.
  - resiliencia por suscripcion (`conf-resilience`) con retry/timeout/bulkhead/circuit-breaker (Resilience4j); `PUT`/`DELETE` de policy exigen `X-Sub-Token`; `GET` sin token.
  - PoC de observabilidad: metricas Micrometer `meg.push.*` (scrape Prometheus `meg_push_*_total`) y logs estructurados `push_outcome` con `subscriptionId/messageId/correlationId/attempt/outcome`.
  - estado `PAUSED_BY_SYSTEM` para suscripciones en degradacion sostenida.
- API y operacion
  - validacion `jakarta.validation` en contratos de topic/subscription/ack.
  - paginado obligatorio en listados.
  - Swagger/OpenAPI con ejemplos por endpoint.
  - Actuator (health, metrics, prometheus).
  - Docker Compose para `app` + `rabbitmq` + `mongodb`.

## 8. Testing

### 8.0 Flujo Git

Antes de cualquier cambio en este repo: crear rama desde `main` actualizado (no commitear en `main`). Convención y pasos completos en el [SPEC del workspace §6](../SPEC.md#6-flujo-git-obligatorio).

### 8.1 Regla para cambios nuevos

Cada **caso de uso nuevo** (regla de negocio, flujo en servicio, validacion relevante o fix con comportamiento acordado) debe incluir **al menos un test unitario** que lo cubra de forma explicita. Los tests de integracion complementan flujos end-to-end; no reemplazan tests unitarios del comportamiento agregado.

### 8.2 Suite actual

Unit tests:
- `TopicServiceTest` — versionado de alta; `updateTopicConfig` con `X-Topic-Token` valido/invalido (`403`).
- `SubscriptionServiceTest` — alta, PULL/ACK/DLQ; `updateSubscription` con `X-Sub-Token` valido/invalido (`401`).
- `MessageServiceTest` — idempotencia por `Idempotency-Key`; validacion de `X-Topic-Token` en publish (via `TopicService.requireTopicWithPublishToken`).
- `TopicSchemaValidationServiceTest` — reglas de schema (`monto`, `cbu`, `du`); `create`/`update` con token; token invalido (`403`); version de topic incorrecta (`400`).
- `ConfResilienceServiceTest` — `updateForSubscription` y `resetForSubscription` rechazan token invalido (`401`).

Integration test (Testcontainers, requiere Docker):
- `PullAckFlowIntegrationTest`
  - RabbitMQ + MongoDB.
  - flujo publicar → pull → ack parcial.
  - flujo PUSH: POST al webhook (MockWebServer) con 200 y cola MAIN vacia.
  - flujo PUSH: 5xx repetidos y luego DLQ (reintentos con `meg.push.max-delivery-attempts=3`).
  - flujo PUSH: 4xx → DLQ inmediato (sin segundo request al mock = sin reintento).
  - flujo PULL: auto-DLQ al alcanzar `maxDeliveryCountPull`.

### 8.3 Ejecucion local (Java 17)

Los tests deben ejecutarse con **Java 17**. Si la terminal tiene otra version por defecto:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
mvn test
```

Opcional en una sola linea:

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) PATH="$JAVA_HOME/bin:$PATH" mvn test
```

## 9. Operacion y configuracion

### 9.1 Modo recomendado

- Docker Compose con servicios `app` + `rabbitmq` + `mongodb`.
- Comandos base:
  - `docker compose up -d --build`
  - `docker compose down -v` (reset total de estado)

### 9.2 Propiedades

- `meg.topic.max-body-bytes` (default `65536`): tamaño maximo por defecto del `payload` en bytes para nuevos topics si no se envia `maxBodyBytes` en create.
- `meg.pull.visibility-timeout-seconds` (default `10`): segundos de invisibilidad para mensajes entregados por PULL sin ACK/REJECT.
- `meg.pull.max-delivery-count-default` (default `10`): cantidad maxima de entregas PULL por mensaje para nuevas suscripciones si no se informa `maxDeliveryCountPull` en create.
- `meg.push.dispatch-interval-ms` (default `2000`): frecuencia del scheduler PUSH.
- `meg.push.max-delivery-attempts` (default `10`): intentos maximos de POST por mensaje PUSH; `0` = usar `maxRetries` de la suscripcion.
- `meg.push.conf-resilience.default.*`: defaults globales de timeout/retry/circuit-breaker/bulkhead cuando una suscripcion PUSH no tiene override en `conf_resilience`.
- `spring.data.mongodb.ttl` (default `2592000` = 30 dias): retencion online de `messages_audit` via TTL.
- `management.endpoints.web.exposure.include` (default `health,info,metrics,prometheus`): endpoints Actuator expuestos por HTTP.

## 10. Seguridad y observabilidad

### 10.1 Seguridad

- El gateway opera detras de API Manager corporativo; no implementa OAuth propio en este servicio.
- Tokens de operacion:
  - `publishToken` (X-Topic-Token) por topic, retornado solo en create.
  - `subToken` (X-Sub-Token) por suscripcion, retornado solo en create.
- Credenciales de Mongo/Rabbit en `application.yml` y `docker-compose.yml` (`admin/admin123`) son **solo demo local**: en ambientes reales deben venir de secret manager.

### 10.2 Observabilidad

- Actuator: `/actuator/health`, `/actuator/metrics`, `/actuator/prometheus`.
- Metricas PUSH (PoC) ver §3.5.2.
- Logs estructurados PUSH: evento `push_outcome` con `subscriptionId/messageId/correlationId/attempt/outcome`.
- Estandar corporativo orientado a **Dynatrace**: pendiente integrar instrumentacion/forwarding (ver §11).

## 11. Roadmap / Pendientes

Lista unificada de items no implementados o que requieren maduracion. Lo que ya esta en codigo se documenta en §7.

### 11.1 Outbox / consistencia auditoria ↔ publicacion

- **Falta:** hoy auditoria (Mongo) y publish (Rabbit) no son transaccionales: ante fallo entre ambos pasos puede haber inconsistencia.
- **Como resolver:** patron Outbox (persistir evento en Mongo dentro de una transaccion logica y publicar asincronamente con worker + reintentos + reconciliacion).

### 11.2 Rate limit en publish

- **Falta:** control de saturacion de publish por ventana temporal.
- **Ambito objetivo:** por **topic** en `POST /topics/{id}/v{version}/messages` (no por suscripcion).
- **Configuracion esperada en topic:**
  - `rateLimitCount` (cantidad maxima de mensajes por ventana)
  - `rateLimitWindow` (numero de unidad temporal)
  - `rateLimitUnit` (`SECONDS` | `MINUTES` | `HOURS` | `DAYS`)
- **Semantica:**
  - si no hay configuracion, no aplica limite;
  - si se supera, responder `429 Too Many Requests` con mensaje claro y, cuando sea posible, `Retry-After`.
- **Estrategia:**
  - Fase PoC: in-memory por topic (simple, una instancia).
  - Fase multi-instancia: backend distribuido (por ejemplo Redis/Bucket4j).

### 11.3 PUSH resiliente nivel produccion

- **Estado actual (PoC):** policies por suscripcion con `retry`, `timeout`, `bulkhead`, `circuit-breaker` + `conf-resilience` + metricas base y logs estructurados (ver §3.5.2).
- **Falta para produccion:**
  - dashboards y alertas operables (SLO/SLA) sobre metricas de resiliencia;
  - trazabilidad distribuida end-to-end (propagacion de trace/contexto hacia webhook);
  - gobernanza de cambios en `conf_resilience` (auditoria y rollback operativo);
  - estrategia robusta multi-instancia para cache de config / invalidez cruzada;
  - campañas de stress/soak/chaos con criterios formales de aceptacion.

### 11.4 Observabilidad operable (Dynatrace)

- **Falta:** integracion real con **Dynatrace** y trazabilidad end-to-end.
- **Como resolver:** instrumentar trazas/metricas/logs con correlation ID, dashboards y alertas (latencia, errores, DLQ, retries).

### 11.5 Gobierno de datos de auditoria (historico > 30 dias)

- **Estado actual:** retencion online resuelta con TTL de 30 dias en `messages_audit` (`spring.data.mongodb.ttl=2592000`).
- **Pendiente opcional:** si negocio requiere historico por topic mas de 30 dias, implementar estrategia de archivado y consulta historica por topic fuera de la coleccion online.

### 11.6 Seguridad del perimetro

- **Falta:** hoy se asume proteccion externa por API Manager corporativo.
- **Como resolver:** mantener API Manager como control principal y evaluar mTLS interno, rotacion automatica de tokens y hardening adicional segun criticidad.

### 11.7 Performance testing maduro

- **Estado actual:** proyecto `performance-testing` con Locust, escenario `degraded` operativo.
- **Pendiente:** escenario `healthy` con SLO, campañas con resultados historicos comparables (tabla/csv), criterios de aceptacion formales por release.
