package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

type Operation struct {
	Op     string `json:"op"` // "put" | "delete"
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
	TS     int64  `json:"ts"`
	NodeID string `json:"node_id"`
}

type Meta struct {
	TS      int64  `json:"ts"`
	NodeID  string `json:"node_id"`
	Deleted bool   `json:"deleted,omitempty"`
}

type suppressor struct {
	mu    sync.Mutex
	until map[string]time.Time
}

func newSuppressor() *suppressor {
	return &suppressor{until: make(map[string]time.Time)}
}

func (s *suppressor) add(key string, d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.until[key] = time.Now().Add(d)
}

func (s *suppressor) shouldSkip(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.until[key]
	if !ok {
		return false
	}
	if time.Now().Before(t) {
		return true
	}
	delete(s.until, key)
	return false
}

func remoteWins(remote Meta, local Meta) bool {
	// (ts_remoto > ts_local) OR (ts_remoto == ts_local AND node_id_remoto > node_id_local)
	if remote.TS > local.TS {
		return true
	}
	if remote.TS < local.TS {
		return false
	}
	return remote.NodeID > local.NodeID
}

func getOrCreateKV(js nats.JetStreamContext, bucket string) (nats.KeyValue, error) {
	kv, err := js.KeyValue(bucket)
	if err == nil {
		return kv, nil
	}
	if errors.Is(err, nats.ErrBucketNotFound) {
		return js.CreateKeyValue(&nats.KeyValueConfig{Bucket: bucket})
	}
	return nil, err
}

func loadMeta(metaKV nats.KeyValue, key string) (Meta, bool) {
	e, err := metaKV.Get(key)
	if err != nil {
		return Meta{}, false
	}
	var m Meta
	if err := json.Unmarshal(e.Value(), &m); err != nil {
		return Meta{}, false
	}
	return m, true
}

func saveMeta(metaKV nats.KeyValue, key string, m Meta) {
	b, err := json.Marshal(m)
	if err != nil {
		log.Printf("meta marshal (%s): %v", key, err)
		return
	}
	if _, err := metaKV.Put(key, b); err != nil {
		log.Printf("meta put (%s): %v", key, err)
	}
}

func publishOp(nc *nats.Conn, subj string, op Operation) {
	b, err := json.Marshal(op)
	if err != nil {
		log.Printf("op marshal (%s/%s): %v", op.Bucket, op.Key, err)
		return
	}
	if err := nc.Publish(subj, b); err != nil {
		log.Printf("publish op (%s/%s): %v", op.Bucket, op.Key, err)
	}
}

// -------------------- Punto 8 (Desafíos) --------------------
// Requerido: "Usar reloj lógico o contador por nodo."
// Implementamos un reloj lógico Lamport persistido en un bucket KV paralelo (<bucket>_clock),
// para que los timestamps sean monotónicos y no dependan del reloj físico.
type Lamport struct {
	mu  sync.Mutex
	val int64
	kv  nats.KeyValue
}

func loadLamport(clockKV nats.KeyValue) int64 {
	e, err := clockKV.Get("clock")
	if err != nil {
		return 0
	}
	v, err := strconv.ParseInt(string(e.Value()), 10, 64)
	if err != nil || v < 0 {
		return 0
	}
	return v
}

func (l *Lamport) persist() {
	if l.kv == nil {
		return
	}
	if _, err := l.kv.Put("clock", []byte(strconv.FormatInt(l.val, 10))); err != nil {
		log.Printf("clock put: %v", err)
	}
}

// Tick genera un nuevo timestamp local (evento local).
func (l *Lamport) Tick() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.val++
	l.persist()
	return l.val
}

// Observe actualiza el reloj al recibir una operación remota.
func (l *Lamport) Observe(remoteTS int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if remoteTS > l.val {
		l.val = remoteTS
	}
	// recibir cuenta como evento
	l.val++
	l.persist()
}

func (l *Lamport) Value() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.val
}

// -------------------- Punto 7: anti-entropy mínima --------------------
func announceLocalState(kv nats.KeyValue, metaKV nats.KeyValue, nc *nats.Conn, repSubj, bucket, fallbackNodeID string, clock *Lamport) {
	// 1) Publicar PUTs para todas las claves existentes en KV.
	keys, err := kv.Keys()
	if err == nil {
		for _, k := range keys {
			e, err := kv.Get(k)
			if err != nil {
				continue
			}

			m, ok := loadMeta(metaKV, k)
			if !ok || m.TS == 0 {
				m = Meta{TS: clock.Tick(), NodeID: fallbackNodeID, Deleted: false}
				saveMeta(metaKV, k, m)
			}

			if m.Deleted {
				publishOp(nc, repSubj, Operation{Op: "delete", Bucket: bucket, Key: k, TS: m.TS, NodeID: m.NodeID})
				continue
			}

			publishOp(nc, repSubj, Operation{
				Op:     "put",
				Bucket: bucket,
				Key:    k,
				Value:  string(e.Value()),
				TS:     m.TS,
				NodeID: m.NodeID,
			})
		}
	}

	// 2) Publicar tombstones (DELETES) que existan solo en metaKV.
	metaKeys, err := metaKV.Keys()
	if err == nil {
		for _, k := range metaKeys {
			m, ok := loadMeta(metaKV, k)
			if !ok || !m.Deleted {
				continue
			}
			publishOp(nc, repSubj, Operation{Op: "delete", Bucket: bucket, Key: k, TS: m.TS, NodeID: m.NodeID})
		}
	}
}

// -------------------- main --------------------

func main() {
	natsURL := flag.String("nats-url", "nats://localhost:4222", "NATS URL")
	bucket := flag.String("bucket", "config", "KV bucket")
	nodeID := flag.String("node-id", "", "Node ID")
	repSubj := flag.String("rep-subj", "rep.kv.ops", "Replication subject")

	announceOnStart := flag.Bool("announce-on-start", true, "P7: publicar estado local al arrancar")
	announceOnReconnect := flag.Bool("announce-on-reconnect", true, "P7: publicar estado local tras reconectar a NATS")

	flag.Parse()

	if *nodeID == "" {
		log.Fatal("Falta --node-id")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Para el handler de reconexión necesitamos que KV/metaKV/clock estén inicializados.
	var (
		kv      nats.KeyValue
		metaKV  nats.KeyValue
		clockKV nats.KeyValue
		clock   Lamport
		nc      *nats.Conn
	)
	ready := make(chan struct{})

	conn, err := nats.Connect(*natsURL, nats.ReconnectHandler(func(_ *nats.Conn) {
		if !*announceOnReconnect {
			return
		}
		select {
		case <-ready:
			announceLocalState(kv, metaKV, nc, *repSubj, *bucket, *nodeID, &clock)
		default:
		}
	}))
	if err != nil {
		log.Fatalf("nats.Connect: %v", err)
	}
	nc = conn
	defer nc.Drain()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("JetStream: %v", err)
	}

	kv, err = getOrCreateKV(js, *bucket)
	if err != nil {
		log.Fatalf("KeyValue(%s): %v", *bucket, err)
	}

	metaBucket := *bucket + "_meta"
	metaKV, err = getOrCreateKV(js, metaBucket)
	if err != nil {
		log.Fatalf("KeyValue(%s): %v", metaBucket, err)
	}

	// Punto 8: bucket de reloj lógico
	clockBucket := *bucket + "_clock"
	clockKV, err = getOrCreateKV(js, clockBucket)
	if err != nil {
		log.Fatalf("KeyValue(%s): %v", clockBucket, err)
	}
	clock = Lamport{val: loadLamport(clockKV), kv: clockKV}

	close(ready)

	sup := newSuppressor()

	// 6.2 · Recibir operaciones remotas
	_, err = nc.Subscribe(*repSubj, func(m *nats.Msg) {
		var op Operation
		if err := json.Unmarshal(m.Data, &op); err != nil {
			log.Printf("[%s] op JSON inválido: %v", *nodeID, err)
			return
		}
		if op.Bucket != *bucket {
			return
		}
		if op.NodeID == *nodeID {
			return // ignorar eco local
		}

		// Punto 8: avanzar reloj lógico al recibir (aunque luego no gane).
		clock.Observe(op.TS)

		localMeta, ok := loadMeta(metaKV, op.Key)
		if !ok {
			localMeta = Meta{}
		}
		remoteMeta := Meta{TS: op.TS, NodeID: op.NodeID, Deleted: op.Op == "delete"}

		if !remoteWins(remoteMeta, localMeta) {
			return
		}

		// Evitar que el watcher publique de nuevo el cambio que vamos a aplicar.
		sup.add(op.Key, 2*time.Second)

		switch op.Op {
		case "put":
			if _, err := kv.Put(op.Key, []byte(op.Value)); err != nil {
				log.Printf("[%s] aplicar put remoto %s: %v", *nodeID, op.Key, err)
				return
			}
			saveMeta(metaKV, op.Key, remoteMeta)
			log.Printf("[%s] aplicado remoto: put %s (ts=%d node=%s)", *nodeID, op.Key, op.TS, op.NodeID)

		case "delete":
			if err := kv.Delete(op.Key); err != nil {
				log.Printf("[%s] aplicar delete remoto %s: %v", *nodeID, op.Key, err)
				return
			}
			saveMeta(metaKV, op.Key, remoteMeta)
			log.Printf("[%s] aplicado remoto: delete %s (ts=%d node=%s)", *nodeID, op.Key, op.TS, op.NodeID)

		default:
			log.Printf("[%s] op desconocida: %q", *nodeID, op.Op)
			return
		}
	})
	if err != nil {
		log.Fatalf("Subscribe(%s): %v", *repSubj, err)
	}

	// 6.1 · Vigilar cambios locales
	w, err := kv.WatchAll()
	if err != nil {
		log.Fatalf("WatchAll: %v", err)
	}
	defer w.Stop()

	// Drenar el estado inicial para no republicarlo como "cambio local".
	for {
		e := <-w.Updates()
		if e == nil {
			break
		}
		if _, ok := loadMeta(metaKV, e.Key()); !ok {
			isDel := e.Operation() != nats.KeyValuePut
			ts := clock.Tick()
			saveMeta(metaKV, e.Key(), Meta{TS: ts, NodeID: *nodeID, Deleted: isDel})
		}
	}

	log.Printf("[%s] syncd activo: bucket=%s meta=%s clock=%s subj=%s url=%s (clock=%d)",
		*nodeID, *bucket, metaBucket, clockBucket, *repSubj, *natsURL, clock.Value())

	// Punto 7: al arrancar, anunciamos el estado (anti-entropy).
	if *announceOnStart {
		announceLocalState(kv, metaKV, nc, *repSubj, *bucket, *nodeID, &clock)
	}

	for {
		select {
		case <-ctx.Done():
			return

		case e := <-w.Updates():
			if e == nil {
				continue
			}
			if sup.shouldSkip(e.Key()) {
				continue
			}

			ts := clock.Tick()

			switch e.Operation() {
			case nats.KeyValuePut:
				op := Operation{
					Op:     "put",
					Bucket: *bucket,
					Key:    e.Key(),
					Value:  string(e.Value()),
					TS:     ts,
					NodeID: *nodeID,
				}
				saveMeta(metaKV, op.Key, Meta{TS: op.TS, NodeID: op.NodeID, Deleted: false})

				publishOp(nc, *repSubj, op)
				log.Printf("[%s] publicado local: put %s (ts=%d)", *nodeID, op.Key, op.TS)

			case nats.KeyValueDelete, nats.KeyValuePurge:
				op := Operation{
					Op:     "delete",
					Bucket: *bucket,
					Key:    e.Key(),
					TS:     ts,
					NodeID: *nodeID,
				}
				saveMeta(metaKV, op.Key, Meta{TS: op.TS, NodeID: op.NodeID, Deleted: true})

				publishOp(nc, *repSubj, op)
				log.Printf("[%s] publicado local: delete %s (ts=%d)", *nodeID, op.Key, op.TS)

			default:
				// Ignore other internal ops
			}
		}
	}
}
