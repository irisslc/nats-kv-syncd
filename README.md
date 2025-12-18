# Sincronización KV NATS con CRDT (LWW) + JetStream + Leaf Nodes

### Autores: Iris Maria Lorente Cutanda, Anass Lambaraa Ben Razzouq

Este repositorio implementa el agente **nats-kv-syncd**, que mantiene consistentes varios buckets KV de NATS (uno por sitio) usando un CRDT **LWW-Register** con soporte de **borrados (tombstones)**, **reloj lógico (Lamport)**, **recuperación ante mensajes perdidos (JetStream durable)** y **fusión periódica (anti-entropy)**.

La topología final usa **leaf nodes** para que NATS enrute el subject de replicación entre sitios (desafío adicional 2).

---

## 1) Qué se entrega

- `main.go`: agente CRDT (LWW + tombstones + Lamport + JetStream durable + reconcile).
- `docker-compose.yml` + `nats-*.conf`: despliegue **hub + leafnodes (nats-a / nats-b)**.
- `scripts/`: automatizan puesta en marcha y pruebas.
- Este `README.md` con:
  - cómo ejecutar,
  - explicación CRDT y metadatos,
  - respuestas a cuestiones (6.1, 6.3, 9.1, 9.2, 9.3 y las 8 del 11),
  - resultados de pruebas.

---

## 2) Requisitos

- Ubuntu + Docker + Docker Compose.
- `nats` CLI instalado.
- Go **>= 1.23** (o Go 1.22 con toolchain auto). Recomendado:
  - `GOTOOLCHAIN=go1.23.0+auto`


---

## 3) Arquitectura y leaf nodes (Desafío adicional 2)

Topología:
- `nats-hub`: acepta conexiones leaf.
- `nats-a` y `nats-b`: servidores NATS **locales** (JetStream + KV) y conectan al hub como **leaf nodes**.
- El agente en cada sitio se conecta a su NATS local y replica operaciones por `rep.kv.ops`.

Esto permite que `rep.kv.ops` se enrute entre sitios por NATS (leaf nodes), sin que el agente “sepa” de la topología (solo publica/consume el subject). 

---

## 4) Cómo ejecutar el entorno

### 4.1 Levantar NATS (hub + leafs)
```bash
docker compose up -d
docker ps
```

Verificar que responden:
```bash
nats --server localhost:4222 server info | head
nats --server localhost:5222 server info | head
```

### 4.2 Crear buckets KV por sitio
```bash
nats --server localhost:4222 kv add config || true
nats --server localhost:5222 kv add config || true
```

### 4.3 Ejecutar agentes
**site-a**
```bash
GOTOOLCHAIN=go1.23.0+auto go run .   --nats-url nats://localhost:4222   --bucket config --node-id site-a   --rep-subj rep.kv.ops --rep-stream REPKVOPS --durable syncd-site-a   --announce-on-start=true --announce-on-reconnect=true --reconcile-every=5m
```

**site-b**
```bash
GOTOOLCHAIN=go1.23.0+auto go run .   --nats-url nats://localhost:5222   --bucket config --node-id site-b   --rep-subj rep.kv.ops --rep-stream REPKVOPS --durable syncd-site-b   --announce-on-start=true --announce-on-reconnect=true --reconcile-every=5m
```

---

## 5) Explicación de la lógica CRDT (LWW Register)

### 5.1 Operaciones
El agente replica operaciones en JSON sobre `rep.kv.ops` con el formato: `{op, bucket, key, value?, ts, node_id}`. 

- `op="put"`: asigna valor a una clave.
- `op="delete"`: borra (lógico) una clave.

### 5.2 Regla de merge LWW
Para cada clave se guarda el meta `(ts, node_id, deleted)` y al recibir una operación remota se aplica si:

- `(ts_remoto > ts_local)` o
- `(ts_remoto == ts_local AND node_id_remoto > node_id_local)`

En caso contrario se conserva el valor local.

### 5.3 Deletes seguros (tombstones)
Un `delete` crea/actualiza un meta con `deleted=true` (tombstone), evitando resurrecciones por `put` antiguos tras una partición. 

---

## 6) Estrategia de almacenamiento de metadatos (punto 6.3)

- Bucket paralelo `config_meta` (o `<bucket>_meta`) con JSON:
```json
{"ts": 12, "node_id":"site-a", "deleted": false}
```
Persistimos ahí lo necesario para el merge LWW + tombstones. 

- Bucket `config_clock` (o `<bucket>_clock`) para el contador Lamport persistido (punto 8). 

---

## 7) Respuestas a las cuestiones del enunciado

### 7.1 Punto 6.1 — Vigilar cambios locales
Se usa `kv.WatchAll()` y, ante PUT/DELETE:
1) genera `ts` (Lamport),
2) actualiza `config_meta`,
3) publica operación en `rep.kv.ops`.

Se drena el “estado inicial” del watcher para no republicar todo como cambios nuevos.

### 7.2 Punto 6.3 — Realizar el merge
Se lee meta local en `config_meta`, se compara con meta remoto usando la regla LWW del documento y se aplica `kv.Put`/`kv.Delete` si gana remoto; luego se persiste el meta ganador. 

### 7.3 Punto 9.1 — Reproducción de operaciones (JetStream durable)
**Cómo:** stream para `rep.kv.ops` (p. ej. `REPKVOPS`) + consumer durable pull con ACK explícito; publicación con `js.Publish(...)`. 

**✅ Ventaja:** replay de pendientes al reconectar.  
**⚠️ Inconveniente:** backlog/retención (coste si se queda atrás).

### 7.4 Punto 9.2 — Reconciliación basada en estado
**Cómo:** al reconectar o periódicamente, listar keys/metas y re-publicar PUT/DELETE equivalentes para forzar convergencia.

**✅ Ventaja:** repara divergencias aunque falten operaciones.  
**⚠️ Inconveniente:** más coste (listado/estado completo).

### 7.5 Punto 9.3 — Estrategia híbrida
**Cómo:** JetStream durable + reconcile periódico/al reconectar.

**✅ Ventaja:** robustez alta.  
**⚠️ Inconveniente:** más complejidad y tráfico.

### 7.6 Punto 8 — “¿Cómo? ¿Dónde?” publicar JSON
Se publica en `rep.kv.ops`:
```bash
nats --server localhost:4222 pub rep.kv.ops '{"op":"put","bucket":"config","key":"app/theme","value":"dark","ts":12,"node_id":"site-a"}'
```


---

## 8) Cuestiones de autoevaluación (punto 11)

1. **¿Por qué los CRDT no necesitan consenso global?**  
Porque convergen por merge determinista y propiedades conmutativa/asociativa/idempotente; no hace falta un orden global único.

2. **¿Qué ventaja aporta usar `node_id` como desempate?**  
Determinismo cuando `ts` empata: todas las réplicas eligen el mismo ganador.

3. **¿Qué ocurre si dos sitios tienen relojes desincronizados?**  
Con tiempo físico, el “ganador” puede ser incorrecto; por eso se usa reloj lógico/contador (Lamport) como exige el punto 8.

4. **¿Diferencia entre replicación por operaciones y por estado?**  
Operaciones: envías “lo que pasó”. Estado: envías “cómo estoy” y haces merge.

5. **¿Qué garantiza la idempotencia en una actualización CRDT?**  
Que re-aplicar/reintentar no altera el resultado final, haciendo seguros duplicados/replays.

6. **¿Por qué combinar JetStream con reconciliación periódica?**  
JetStream cubre pérdidas con replay; la reconciliación corrige divergencias si faltan operaciones o hubo cambios fuera del flujo. 

7. **¿Qué pruebas harías para demostrar convergencia tras una partición?**  
Desconectar hub/leaf o un sitio, hacer escrituras concurrentes y reconectar verificando convergencia según LWW (incluyendo deletes). 

8. **¿Diferencias con cr-sqlite?**  
Aquí el CRDT es LWW por clave + mensajería NATS/JetStream; cr-sqlite integra CRDTs en el motor de datos a nivel BD con semánticas más ricas.

---

## 9) Resultados de pruebas (resumen)

- LWW determinista (`ts` + desempate por `node_id`).
- Tombstones: tras delete no reaparece.
- Lamport: `ts` monotónico y persistido.
- JetStream durable: `Unprocessed Messages: 0` tras replay.
- Híbrida: reconcile corrige cambios “fuera de banda”.
- Leaf nodes: escritura en `nats-a` se refleja en `nats-b`. 

---

## 10) Scripts

- `scripts/00_up.sh`: levanta docker + crea buckets.
- `scripts/01_run_agents.sh`: arranca agentes en background (logs en `./logs`).
- `scripts/02_test_leaf_basic.sh`: prueba replicación A->B.
- `scripts/03_test_partition_hub.sh`: partición parando `nats-hub`.
- `scripts/04_test_js_replay.sh`: replay durable con B parado.
- `scripts/05_test_hybrid_reconcile.sh`: cambio fuera de banda + reconcile.
- `scripts/99_down.sh`: para docker y limpia.


