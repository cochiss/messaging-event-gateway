# ARQEVENT — Estándares de integración y buenas prácticas (MEG)

Guía técnica para equipos que integran aplicaciones con el **Messaging Event Gateway (MEG)**. Cubre diseño de soluciones basadas en eventos, consumo PULL/PUSH, librería cliente, manejo de errores, trazabilidad y operación.

**Audiencia:** desarrolladores, analistas técnicos y operadores que implementan productores o consumidores de eventos.

**Relación con Jira (SEPDRE-20737):**

| Subtarea | Documento |
|----------|-----------|
| SEPDRE-20742 — Patrones de integración | [01-patrones-push-vs-pull.md](01-patrones-push-vs-pull.md) |
| SEPDRE-20743 — Buenas prácticas de consumo | [02-buenas-practicas-consumo-y-traza.md](02-buenas-practicas-consumo-y-traza.md) |
| SEPDRE-20744 — Guía de uso de librerías | [03-guia-libreria-pull-consumer-lib.md](03-guia-libreria-pull-consumer-lib.md) |
| SEPDRE-20745 — Errores comunes y anti-patterns | [04-errores-comunes-anti-patterns-y-operacion.md](04-errores-comunes-anti-patterns-y-operacion.md) |

## Ruta de lectura recomendada

1. **Patrones PUSH vs PULL** — entender el modelo mental y cuándo usar cada modo.
2. **Buenas prácticas de consumo y traza** — `eventType`, correlation, idempotency, schemas, responsabilidad por consumer.
3. **Guía de la librería** — cómo la lib abstrae GET/ACK, YAML y pipeline de procesamiento.
4. **Errores, anti-patterns y operación** — DLQ, deploy, Resilience4j, runbooks.

## Documentación de referencia (contrato y código)

| Recurso | Ubicación |
|---------|-----------|
| Contrato API del gateway | [`SPEC.md`](../SPEC.md) |
| Ejemplos curl / Postman | [`README.md`](../README.md) |
| Librería PULL | [`pull-consumer-lib` SPEC](https://github.com/cochiss/pull-consumer-lib/blob/main/SPEC.md) |
| PoC de consumidor | [`sample-consumer` app SPEC](https://github.com/cochiss/sample-consumer/blob/main/app/SPEC.md) |
| Demo multi-repo (workspace local) | Árbol `demo-messaging-event-gateway` con gateway, lib y sample como carpetas hermanas |

## Cambio de mentalidad: de “cola” a “evento + suscripción”

En integraciones clásicas suele pensarse en “leer de una cola”. En MEG el modelo es:

```
Productor → Topic (versionado) → Suscripción(es) → Consumer (PULL o PUSH)
```

- Un **topic** es el canal lógico de publicación (con versión explícita).
- Cada **suscripción** es un consumidor distinto con **una responsabilidad** (nombre, cola, token propio).
- El mismo evento puede fan-out a varias suscripciones; **cada una procesa de forma independiente**.

Diseñar “un consumer que hace todo” va contra el modelo: preferir **varias suscripciones pequeñas** (como en el sample: `pago-reintegros`, `pago-reintegros-alto-monto`, `notificacion-pago-reintegros`).
