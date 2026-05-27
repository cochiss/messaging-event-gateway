# 04 — Errores comunes, anti-patterns y operación

## 1. Anti-patterns más frecuentes

### 1.1 Consumer “god object”

Síntoma: una sola suscripción hace validación, pago, conciliación, notificación, auditoría, retries custom y ruteo complejo.

Impacto:

- difícil de testear,
- difícil de operar,
- cambios de un caso rompen otros.

Corrección: dividir por suscripción y responsabilidad funcional.

### 1.2 Sin idempotencia en consumidor

Síntoma: el mismo mensaje genera efectos duplicados.

Impacto: pagos dobles, doble inserción, inconsistencias.

Corrección: deduplicar por `messageId` o clave de negocio estable antes de ejecutar efectos.

### 1.3 Ignorar trazabilidad

Síntoma: logs sin correlation id/source app/eventType.

Impacto: incidentes largos y difíciles de reconstruir.

Corrección: log estructurado y propagación obligatoria de metadatos.

### 1.4 Requeue ciego de DLQ

Síntoma: vaciar DLQ sin análisis.

Impacto: loop de reintentos, saturación, ruido operativo.

Corrección: clasificar causa raíz, corregir, requeue selectivo.

## 2. Errores HTTP en PUSH: 400 vs 500

Regla de oro:

- **4xx** = error permanente de datos/negocio => DLQ inmediata.
- **5xx/timeout/red** = error transitorio => reintentos; luego DLQ si se agotan intentos.

### Ejemplos de 4xx (sin reintento)

- payload inválido,
- firma/token de webhook inválido,
- regla de negocio definitiva (ej. entidad inexistente no recuperable).

### Ejemplos de 5xx/transitorio

- dependencia caída temporal,
- timeout de base de datos,
- error de infraestructura temporal.

Si devolvés 500 por algo que en realidad es 400, vas a generar retries innecesarios y presión operativa.

## 3. Cómo programar cuando algo va a DLQ

### 3.1 Estrategia recomendada

1. Detectar y registrar causa (`messageId`, `correlationId`, `eventType`, `sourceApp`).
2. Clasificar: transitorio vs permanente.
3. Corregir causa raíz.
4. Reprocesar selectivamente (requeue puntual o batch controlado).
5. Medir reincidencia para evitar “loop de DLQ”.

### 3.2 Runbook mínimo

- Dashboard: volumen DLQ por suscripción y motivo.
- Alerta: umbral de DLQ/minuto.
- Procedimiento: quién triagea, quién corrige, quién habilita requeue.
- Postmortem: causa, fix y mejora preventiva.

## 4. Resilience4j / conf-resilience

Aplica a **PUSH** y se configura por suscripción (`conf-resilience`):

- `retry` (intentos, backoff, jitter),
- `timeout` por llamada,
- `bulkhead` para concurrencia,
- `circuitBreaker` para degradación sostenida.

### ¿Cuándo se activa?

- ante fallos 5xx/red/timeout repetidos,
- cuando se supera umbral del circuit breaker,
- cuando hay saturación de concurrencia (bulkhead).

### Estado técnico de suscripción

- `PAUSED_BY_SYSTEM` lo gestiona resiliencia automática.
- El operador no debería setear ese estado manualmente vía API.

## 5. Operación en deploy: manejo de suscripciones

Para cambios con riesgo de incompatibilidad o ventana de mantenimiento:

1. pasar suscripción a `INACTIVE` (si aplica),
2. desplegar,
3. validar salud/observabilidad,
4. volver a `ACTIVE`,
5. monitorear backlog y DLQ.

Esto evita procesar mensajes con versión de código intermedia o parcialmente desplegada.

### ¿Siempre hay que inactivar?

No necesariamente. Depende de:

- si el cambio es backward compatible,
- si hay blue/green/canary,
- tolerancia del negocio a reintentos/reprocesos.

Si hay duda, preferir inactivar temporalmente la suscripción crítica.

## 6. Manejo de versiones

### Topic versioning

- El path versiona contrato: `/topics/{id}/v{version}/...`.
- Evoluciones incompatibles deben ir a nueva versión.
- No mezclar “romper contrato” dentro de la misma versión.

### Event versioning

- `eventVersion` en el mensaje permite evolucionar el evento sin romper consumidores inmediatamente.
- Mantener compatibilidad durante transición y retirar versiones viejas de forma planificada.

## 7. Lista de verificación pre-producción

- [ ] Cada consumer tiene responsabilidad clara.
- [ ] Reglas 4xx/5xx definidas y testeadas en webhook PUSH.
- [ ] Idempotencia implementada y validada con reintentos.
- [ ] Correlation/source/eventType presentes en logs y métricas.
- [ ] Política de DLQ/requeue documentada.
- [ ] Estrategia de deploy de suscripciones acordada (`ACTIVE/INACTIVE`).
- [ ] Plan de versionado de topic/event documentado.

## 8. Señales de mala salud operativa

- crecimiento sostenido de DLQ,
- muchos retries con baja tasa de éxito,
- consumidores que quedan pendientes sin ACK por largo tiempo,
- falta de correlación entre productor y consumidor en logs.

Ante estas señales: pausar impacto (INACTIVE si aplica), contener, diagnosticar y recién después reprocesar.
