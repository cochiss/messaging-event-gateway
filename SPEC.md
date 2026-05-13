# Messaging Event Gateway - SPEC

## 1. Objetivo

Proveer una API de mensajeria desacoplada sobre RabbitMQ con:
- topics logicos versionados,
- suscripciones por consumidor,
- modo PULL/PUSH,
- auditoria y control en MongoDB.

## 2. Stack

- Kotlin + Spring Boot 3.2.4
- RabbitMQ (topic exchange + queue por subscription)
- MongoDB
- Resilience4j
- Springdoc OpenAPI
- Spring Boot Actuator
- Bean Validation (`jakarta.validation`) en contratos API

## 3. Modelo funcional

### 3.0 Semantica de entrega (contrato)
- Garantia del gateway: **at-least-once** para PULL y PUSH.
- Pueden existir duplicados por reintentos, redelivery y fallos de red/transitorios.
- Los consumidores deben ser idempotentes; no se garantiza exactly-once end-to-end.

### 3.1 Publicacion
1. Productor publica en `POST /topics/{id}/v{version}/messages`
2. Se audita en Mongo (`messages_audit`)
3. Se publica en exchange `ex.{topicId}`
4. Rabbit distribuye a colas de suscripciones (internas de infraestructura)

### 3.2 Consumo PULL
1. Cliente consulta `GET /subscriptions/{id}/messages`
2. Servicio entrega mensajes pendientes y/o consume de Rabbit
3. Los mensajes entregados quedan invisibles por 10 segundos (visibility timeout) si no se hace ACK/REJECT
3. Cliente confirma con `PUT .../messages/{messageId}`

#### 3.2.1 Detalle de Visibility Timeout
- Al entregar un mensaje por PULL, el sistema persiste/actualiza en `subscription_pending_messages`:
  - `visibilityUntil`: timestamp hasta el cual el mensaje permanece invisible.
  - `deliveryCount`: contador de reentregas.
- Regla de visibilidad:
  - Si `visibilityUntil` es `null` o ya vencio, el mensaje es visible.
  - Si `visibilityUntil` esta en el futuro, el mensaje no se entrega en nuevos pulls.
- No se usa cron para "desinvisibilizar": la visibilidad se resuelve por lectura (lazy evaluation) comparando `visibilityUntil` con `now`.
- Si el consumidor confirma:
  - `PROCESSED`: se elimina el pending.
  - `REJECT`: se mueve a DLQ y se elimina el pending de MAIN.
- Si no confirma, el mensaje vuelve a estar disponible automaticamente al vencer `visibilityUntil`.
- Si `deliveryCount` alcanza `maxDeliveryCountPull` de la suscripcion, el mensaje se mueve automaticamente a DLQ con motivo `max-delivery-exceeded` y deja de entregarse por PULL.

### 3.3 DLQ y reproceso
- `REJECT` mueve mensaje de MAIN a DLQ
- `GET /subscriptions/{id}/dead-letters` lista dead letters
- `POST .../dead-letters/{messageId}/requeue` reencola a MAIN

### 3.4 Envio PUSH (webhook)
- Un scheduler despacha mensajes de la cola MAIN de cada suscripcion **PUSH** hacia `urlRest` (POST con el cuerpo del mensaje).
- **Respuesta HTTP 2xx:** el mensaje se considera entregado (no va a DLQ).
- **Respuesta HTTP 4xx:** se interpreta como error **definitivo del lado destino** (payload invalido, auth, etc.): el mensaje se envia **de inmediato** a la cola **DLQ** de esa suscripcion, **sin** reintentos ni contador de intentos.
- **Respuesta HTTP 5xx, 3xx no exitosas, o fallo de red** (timeout, conexion rechazada, etc.): **reintentos** — el mensaje vuelve a MAIN con cabecera `x-meg-push-attempt` hasta `meg.push.max-delivery-attempts` (o `maxRetries` de la suscripcion si la propiedad es 0); agotados los intentos, va a **DLQ**.
- Intervalo entre ciclos de despacho: `meg.push.dispatch-interval-ms` (default 2000 ms).

#### 3.4.1 Resiliencia PUSH por suscripcion (`conf-resilience`)
- Se define configuracion de resiliencia por suscripcion PUSH en almacenamiento persistente (`conf_resilience`) + cache en memoria al arrancar.
- El path de administracion por suscripcion es: `.../conf-resilience`.
- Al crear una suscripcion `type=PUSH` (`POST /subscriptions`), se crea automaticamente su documento en `conf_resilience` con defaults.
- El estado `INACTIVE` de suscripcion no se usa como automatismo tecnico de Resilience4j; la resiliencia tecnica se gobierna por policy (`retry`, `timeout`, `bulkhead`, `circuit-breaker`).
- Politica recomendada de ejecucion:
  1. `bulkhead` (limite de concurrencia por suscripcion)
  2. `timeout` de llamada HTTP
  3. `retry` con backoff+jitter para errores transitorios (5xx/red/timeout)
  4. `circuit-breaker` para proteger destino y gateway ante degradacion sostenida
- Semantica:
  - 2xx: delivered
  - 4xx: DLQ inmediato, sin retry
  - 5xx/red/timeout: retry segun policy; al agotar, DLQ
  - circuit abierto: se evita intento real al webhook mientras dure ventana OPEN

#### 3.4.2 Alcance PoC de resiliencia PUSH
- Para la PoC se define como obligatorio:
  - politicas por suscripcion con `conf-resilience` (`retry`, `timeout`, `bulkhead`, `circuit-breaker`);
  - metricas minimas por suscripcion (IDs Micrometer `meg.push.*`; en scrape Prometheus aparecen como `meg_push_*_total`):
    - `meg.push.deliveries.ok` → `meg_push_deliveries_ok_total`
    - `meg.push.deliveries.failed` → `meg_push_deliveries_failed_total`
    - `meg.push.retries` → `meg_push_retries_total`
    - `meg.push.dlq` → `meg_push_dlq_total` (tag `outcome` cuando aplica)
    - `meg.push.circuit.open` → `meg_push_circuit_open_total`
  - Hasta el primer evento que incremente cada contador, puede no figurar línea en `/actuator/prometheus` (registro perezoso de Micrometer).
  - logs estructurados por intento/resultado con:
    - `subscriptionId`
    - `messageId`
    - `correlationId`
    - `attempt`
    - `outcome`
- Outcomes esperados para PoC:
  - `delivered`
  - `retry_scheduled`
  - `dlq`
  - `4xx_direct_dlq`
  - `retry_exhausted_dlq`
- Criterio de aceptacion PoC:
  - con webhook degradado, el gateway no cae, los mensajes siguen una politica deterministica (retry o DLQ segun tipo de error) y la suscripcion puede transicionar a `PAUSED_BY_SYSTEM`.

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
  - `maxBodyBytes` es opcional; si no se envía, usa `meg.topic.max-body-bytes`.

- `GET /topics?page=0&size=10`
  - respuesta sanitizada: NO incluye `publishToken`.
  - `publishToken` solo se devuelve en la respuesta del `POST /topics` (al momento de la creacion). Guardarlo en ese momento; no es consultable luego.

- `PUT /topics/{id}`
  - Permite actualizar solo:
    - `description`
    - `ownerApp`
    - `maxBodyBytes`
  - Request:
```json
{
  "description": "Topic de solicitudes de reintegros actualizado",
  "ownerApp": "finanzas-api-v2",
  "maxBodyBytes": 131072
}
```

- `POST /topics/{id}/v{version}/schema-validation`
  - Crea schema validation para un topic/version.
  - Si no existe schema para un topic/version, el publish no valida schema.
- `PUT /topics/{id}/v{version}/schema-validation`
  - Actualiza schema validation existente.
- `GET /topics/{id}/v{version}/schema-validation`
  - Obtiene schema validation configurado.
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
      "du": {
        "type": "string",
        "minLength": 7,
        "maxLength": 8,
        "pattern": "^[0-9]+$"
      }
    },
    "additionalProperties": true
  }
}
```

### 4.2 Messages

- `POST /topics/{id}/v{version}/messages`
  - Headers obligatorios: `Idempotency-Key`, `X-Topic-Token`, `X-Correlation-Id`, `X-Source-App`.
  - `X-Topic-Token` debe coincidir con el token del topic; si no coincide, responde `403`.
  - Body: `eventType` obligatorio y `eventVersion` opcional (default `1`).
  - Valida tamaño del `payload` contra `topic.maxBodyBytes`.
  - Si existe schema validation habilitado para ese `topicId`/`topicVersion`, valida `payload` contra JSON schema (draft-07).
  - Si excede, responde `413 Payload Too Large` con mensaje claro:
    - `payload size 81234 exceeds maxBodyBytes 65536 for topic solicitudes-reintegros`

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
  - Response relevante:
```json
{
  "id": "solicitudes-reintegros-v2-procesador-v2",
  "token": "uuid-token",
  "mainQueue": "q.solicitudes-reintegros.v2.procesador-v2",
  "dlq": "q.solicitudes-reintegros.v2.procesador-v2.dlq"
}
```

- `GET /subscriptions?page=0&size=10`
  - respuesta sanitizada: no incluye `token`.
  - filtro opcional: `topicId`
- `PUT /subscriptions/{id}`
  - Permite actualizar:
    - `description`
    - `status` (`ACTIVE` o `INACTIVE`)
    - `urlRest` (solo para `PUSH`; en `PULL` debe ser nulo/vacío)
    - `maxDeliveryCountPull` (límite de reentregas PULL antes de DLQ automático)
  - Estado adicional del sistema:
    - `PAUSED_BY_SYSTEM` (solo gestionado internamente por resiliencia PUSH; no se setea manualmente por API de update standard).

### 4.4 Pull/Ack/DLQ

- `GET /subscriptions/{id}/messages?page=0&size=10`
  - `rows` puede usarse como alias de `size` para definir cantidad.
- `PUT /subscriptions/{id}/messages/{messageId}`
  - Request:
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

- `GET /subscriptions/{id}/conf-resilience`
  - Obtiene policy persistida de resiliencia para la suscripcion PUSH.
  - En suscripciones PUSH creadas por API, siempre existe documento porque se crea en `POST /subscriptions`.
- `PUT /subscriptions/{id}/conf-resilience`
  - Actualiza policy de resiliencia para la suscripcion PUSH.
- `DELETE /subscriptions/{id}/conf-resilience`
  - Reinicia policy de la suscripcion a defaults (no elimina la fila/documento).

Reglas:
- Aplica solo a suscripciones `type=PUSH`.
- Si la suscripcion no existe: `404`.
- Si la suscripcion no es PUSH: `400`.
- Defaults iniciales al crear suscripcion PUSH:
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
- Validaciones minimas:
  - `timeoutMs > 0`
  - `retry.maxAttempts >= 1`
  - `retry.multiplier >= 1.0`
  - `circuitBreaker.failureRateThreshold` y `slowCallRateThreshold` en `1..100`
  - `circuitBreaker.minimumNumberOfCalls <= slidingWindowSize`
  - `bulkhead.maxConcurrentCalls >= 1`

Header obligatorio:
- `X-Sub-Token`
- `Idempotency-Key` (obligatorio en publish)
- `X-Topic-Token` (obligatorio en publish)
- `X-Correlation-Id` (obligatorio en publish)
- `X-Source-App` (obligatorio en publish)

## 5. Reglas de negocio

- Estandar de nombres para `topicId` y `nameSub`:
  - solo letras y guion medio (kebab-case): `^[a-z]+(-[a-z]+)*$`
  - maximo 50 caracteres.
  - no se permiten espacios, underscores, puntos ni numeros.
  - el gateway normaliza automaticamente a lowercase antes de persistir/operar.
- `Topic.version` obligatoria.
- Versionado secuencial por topic:
  - primera alta: `version = 1`
  - siguiente alta: `current + 1`
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
- En PULL, un mensaje leido sin ACK/REJECT permanece invisible por 10 segundos y luego vuelve a estar disponible.
- En PULL, `maxDeliveryCountPull` define cuantas veces se puede volver a entregar un mismo mensaje sin ACK/REJECT antes de moverlo automaticamente a DLQ.
- Default global de `maxDeliveryCountPull` para nuevas suscripciones: `meg.pull.max-delivery-count-default` (por defecto `10`).
- En publish, si llega `Idempotency-Key` repetido para el mismo topic/version, el gateway evita duplicados y responde con el `messageId` ya registrado.
- En publish, `X-Topic-Token` debe coincidir con el token configurado del topic.
- En publish, `eventType` es obligatorio y `eventVersion` es opcional con default `1`.
- En publish, el `payload` no puede exceder `topic.maxBodyBytes`.
- En publish, si hay schema validation habilitado y el `payload` no cumple, responde `400` y no publica.
- Todo consumidor debe implementar idempotencia para manejar potenciales duplicados por semantica at-least-once.
- Estrategia recomendada de consumidor: deduplicar por `messageId` o clave de negocio estable antes de ejecutar efectos.

## 5.1 Buenas practicas API

- Todo endpoint `GET` debe implementar paginado obligatorio mediante `page` y `size`.
- `page` es base 0.
- `size` debe tener limite razonable definido por implementacion para evitar lecturas masivas.
- Validacion de contratos en capa controller con `@Valid`/constraints para requests de Topic/Subscription/Ack.
- Excepcion explicita: `POST /topics/{id}/v{version}/messages` mantiene `payload` libre por defecto, pero puede validarse con schema opcional por topic/version.
- Errores API deben responder mensajes claros y accionables (evitar `Bad Request` genérico sin detalle).
- Formato de error estandarizado:
```json
{
  "error": "VALIDATION_ERROR",
  "message": "Missing required header: Idempotency-Key"
}
```
- Casos esperados:
  - Header faltante `Idempotency-Key` -> 400 + mensaje explicito.
  - Header vacio `Idempotency-Key` -> 400 + mensaje explicito.
  - Header faltante `X-Topic-Token`, `X-Correlation-Id` o `X-Source-App` -> 400 + mensaje explicito.
  - `X-Topic-Token` invalido -> 403 + mensaje explicito.
  - `eventType` vacio -> 400 + mensaje explicito.
  - `eventVersion < 1` -> 400 + mensaje explicito.
  - Fallos de validacion de body/params -> 400 + detalle del campo/regla.

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
- topic versioning y validaciones de secuencia
- unicidad de subscriptions
- pull persistente
- ack por mensaje
- dlq + requeue funcional
- listados de topics y subscriptions

## 8. Testing

### 8.0 Regla para cambios nuevos

Cada **caso de uso nuevo** (regla de negocio, flujo en servicio, validación relevante o fix con comportamiento acordado) debe incluir **al menos un test unitario** que lo cubra de forma explícita. Los tests de integración listados abajo complementan flujos end-to-end; no reemplazan tests unitarios del comportamiento agregado.

Unit tests:
- `TopicServiceTest`
- `SubscriptionServiceTest`
- `MessageServiceTest` (incluye idempotencia por `Idempotency-Key`)
- `TopicSchemaValidationServiceTest` (valida reglas de schema para `monto`, `cbu`, `du`)

Integration test (Testcontainers):
- `PullAckFlowIntegrationTest`
  - RabbitMQ + MongoDB
  - flujo publicar -> pull -> ack parcial
  - flujo PUSH: POST al webhook (MockWebServer) con respuesta 200 y cola MAIN vacía
  - flujo PUSH: tres respuestas 5xx y luego DLQ (reintentos con `meg.push.max-delivery-attempts=3`)
  - flujo PUSH: un 4xx y DLQ (sin segundo request al mock = sin reintento)

Ejecución: desde esta carpeta, `mvn clean test` (requiere Docker para Testcontainers).

Importante (entorno local): los tests deben ejecutarse con **Java 17**.
Si la terminal tiene otra versión por defecto (por ejemplo Java 8), forzar antes de correr:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
mvn test
```

Opcional en una sola línea:

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) PATH="$JAVA_HOME/bin:$PATH" mvn test
```

## 9. Operacion

Modo recomendado:
- Docker Compose (`app`, `rabbitmq`, `mongodb`)

Comandos base:
- `docker compose up -d --build`
- `docker compose down -v` (reset total de estado)

Configuraciones:
- `meg.topic.max-body-bytes` (default: `65536`): tamaño máximo por defecto del `payload` en bytes para nuevos topics si no se envía `maxBodyBytes` en create.
- `meg.pull.visibility-timeout-seconds` (default: `10`): segundos de invisibilidad para mensajes entregados por PULL sin ACK/REJECT.
- `meg.pull.max-delivery-count-default` (default: `10`): cantidad máxima de entregas PULL por mensaje para nuevas suscripciones si no se informa `maxDeliveryCountPull` en create.
- `meg.push.dispatch-interval-ms` (default: `2000`): frecuencia del scheduler PUSH.
- `meg.push.max-delivery-attempts` (default en `application.yml`: `10`): intentos maximos de POST por mensaje PUSH; `0` = usar `maxRetries` de la suscripcion.
- `meg.push.conf-resilience.default.*`: defaults globales para timeout/retry/circuit-breaker/bulkhead cuando una suscripcion PUSH no tiene override en `conf_resilience`.
- `spring.data.mongodb.ttl` (default en `application.yml`: `2592000` = 30 dias): retencion online de `messages_audit` via TTL.
- `management.endpoints.web.exposure.include` (default en este proyecto: `health,info,metrics,prometheus`): endpoints de Actuator expuestos por HTTP.

## 10. Seguridad y observabilidad

- Seguridad de acceso: el gateway opera detras de API Manager corporativo; no implementa OAuth propio en este servicio por ahora.
- Endpoints operativos: Actuator habilitado (`/actuator/health`, `/actuator/metrics`, `/actuator/prometheus`).
- Observabilidad: estandar corporativo orientado a Dynatrace; pendiente de integrar instrumentacion/forwarding segun plataforma destino.

## 10.1 Pendiente opcional de auditoria historica

- Con TTL de 30 dias, `messages_audit` mantiene solo auditoria online reciente.
- Si negocio requiere historico por topic (mas de 30 dias), se debe implementar estrategia de archivado y consulta historica por topic fuera de la coleccion online.

## 11. Pendiente: rate limit en publish

Para implementar rate limit de forma consistente con el modelo actual, se define este alcance objetivo:

- Ambito inicial: **por topic** en `POST /topics/{id}/v{version}/messages` (no por suscripcion).
- Configuracion esperada en topic:
  - `rateLimitCount` (cantidad maxima de mensajes por ventana)
  - `rateLimitWindow` (numero de unidad temporal)
  - `rateLimitUnit` (`SECONDS` | `MINUTES` | `HOURS` | `DAYS`)
- Semantica:
  - si no hay configuracion, no aplica limite;
  - si se supera el limite, responder `429 Too Many Requests` con mensaje claro y, cuando sea posible, `Retry-After`.
- Estrategia de implementacion:
  - Fase PoC: in-memory por topic (simple, una instancia).
  - Fase multi-instancia: backend distribuido (por ejemplo Redis/Bucket4j) para consistencia entre nodos.
