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

// -------------------- Punto 8: reloj lógico Lamport --------------------
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

func (l *Lamport) Tick() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.val++
	l.persist()
	return l.val
}

func (l *Lamport) Observe(remoteTS int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if remoteTS > l.val {
		l.val = remoteTS
	}
	l.val++
	l.persist()
}

func (l *Lamport) Value() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.val
}

// -------------------- Punto 9: JetStream duradero para rep.kv.ops --------------------
func ensureRepStream(js nats.JetStreamContext, streamName, subj string) error {
	if _, err := js.StreamInfo(streamName); err == nil {
		return nil
	} else if !errors.Is(err, nats.ErrStreamNotFound) {
		return err
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      streamName,
		Subjects:  []string{subj},
		Storage:   nats.FileStorage, // persistente (dentro del servidor)
		Retention: nats.LimitsPolicy,
		MaxAge:    7 * 24 * time.Hour, // ajustable
	})
	return err
}

func jsPublishOp(js nats.JetStreamContext, subj string, op Operation) {
	b, err := json.Marshal(op)
	if err != nil {
		log.Printf("op marshal (%s/%s): %v", op.Bucket, op.Key, err)
		return
	}
	if _, err := js.Publish(subj, b); err != nil {
		log.Printf("js.Publish op (%s/%s): %v", op.Bucket, op.Key, err)
	}
}

// -------------------- Punto 7: anti-entropy mínima (estado) --------------------
func announceLocalState(kv nats.KeyValue, metaKV nats.KeyValue, js nats.JetStreamContext, repSubj, bucket, fallbackNodeID string, clock *Lamport) {
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
				jsPublishOp(js, repSubj, Operation{Op: "delete", Bucket: bucket, Key: k, TS: m.TS, NodeID: m.NodeID})
				continue
			}

			jsPublishOp(js, repSubj, Operation{
				Op:     "put",
				Bucket: bucket,
				Key:    k,
				Value:  string(e.Value()),
				TS:     m.TS,
				NodeID: m.NodeID,
			})
		}
	}

	metaKeys, err := metaKV.Keys()
	if err == nil {
		for _, k := range metaKeys {
			m, ok := loadMeta(metaKV, k)
			if !ok || !m.Deleted {
				continue
			}
			jsPublishOp(js, repSubj, Operation{Op: "delete", Bucket: bucket, Key: k, TS: m.TS, NodeID: m.NodeID})
		}
	}
}

func main() {
	natsURL := flag.String("nats-url", "nats://localhost:4222", "NATS URL")
	bucket := flag.String("bucket", "config", "KV bucket")
	nodeID := flag.String("node-id", "", "Node ID")
	repSubj := flag.String("rep-subj", "rep.kv.ops", "Replication subject")

	// P7/P9: reconciliación por estado
	announceOnStart := flag.Bool("announce-on-start", true, "Publicar estado local al arrancar")
	announceOnReconnect := flag.Bool("announce-on-reconnect", true, "Publicar estado local tras reconectar a NATS")

	// P9.4 (adicional): reconciliación periódica. 0 = desactivado.
	reconcileEvery := flag.Duration("reconcile-every", 5*time.Minute, "P9: ejecutar reconcile periódicamente; 0 para desactivar")

	// P9.1: stream + consumer durable
	repStream := flag.String("rep-stream", "REPKVOPS", "P9: nombre del stream JetStream para rep-subj")
	durable := flag.String("durable", "", "P9: durable consumer (por defecto syncd-<node-id>)")
	fetchBatch := flag.Int("fetch-batch", 64, "P9: batch size para PullSubscribe/Fetch")
	fetchWait := flag.Duration("fetch-wait", 500*time.Millisecond, "P9: MaxWait por Fetch")

	flag.Parse()

	if *nodeID == "" {
		log.Fatal("Falta --node-id")
	}
	if *durable == "" {
		*durable = "syncd-" + *nodeID
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reconCh := make(chan struct{}, 1)

nc, err := nats.Connect(*natsURL, nats.ReconnectHandler(func(_ *nats.Conn) {
	// Punto 9.3: además del replay (consumer durable), forzamos una reconciliación
	// basada en estado cuando vuelve la conectividad.
	if !*announceOnReconnect {
		return
	}
	select {
	case reconCh <- struct{}{}:
	default:
	}
}))
	if err != nil {
		log.Fatalf("nats.Connect: %v", err)
	}
	defer nc.Drain()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("JetStream: %v", err)
	}

	kv, err := getOrCreateKV(js, *bucket)
	if err != nil {
		log.Fatalf("KeyValue(%s): %v", *bucket, err)
	}
	metaBucket := *bucket + "_meta"
	metaKV, err := getOrCreateKV(js, metaBucket)
	if err != nil {
		log.Fatalf("KeyValue(%s): %v", metaBucket, err)
	}

	clockBucket := *bucket + "_clock"
	clockKV, err := getOrCreateKV(js, clockBucket)
	if err != nil {
		log.Fatalf("KeyValue(%s): %v", clockBucket, err)
	}
	clock := Lamport{val: loadLamport(clockKV), kv: clockKV}

	// P9.1: stream persistente para rep.kv.ops
	if err := ensureRepStream(js, *repStream, *repSubj); err != nil {
		log.Fatalf("ensureRepStream(%s): %v", *repStream, err)
	}

	// P9.1: consumidor durable (Pull)
	sub, err := js.PullSubscribe(*repSubj, *durable,
		nats.BindStream(*repStream),
		nats.ManualAck(),
	)
	if err != nil {
		log.Fatalf("PullSubscribe(%s, durable=%s): %v", *repSubj, *durable, err)
	}

	sup := newSuppressor()

	// Consumidor durable: JetStream entregará pendientes tras reconectar.
	go func() {
		for ctx.Err() == nil {
			msgs, err := sub.Fetch(*fetchBatch, nats.MaxWait(*fetchWait))
			if err != nil {
				if errors.Is(err, nats.ErrTimeout) {
					continue
				}
				log.Printf("[%s] fetch err: %v", *nodeID, err)
				continue
			}
			for _, m := range msgs {
				func() {
					defer func() { _ = m.Ack() }()

					var op Operation
					if err := json.Unmarshal(m.Data, &op); err != nil {
						log.Printf("[%s] op JSON inválido: %v", *nodeID, err)
						return
					}
					if op.Bucket != *bucket {
						return
					}

					// P8: avanzar reloj al recibir
					clock.Observe(op.TS)

					// Ignorar eco local, pero ACK igualmente
					if op.NodeID == *nodeID {
						return
					}

					localMeta, ok := loadMeta(metaKV, op.Key)
					if !ok {
						localMeta = Meta{}
					}
					remoteMeta := Meta{TS: op.TS, NodeID: op.NodeID, Deleted: op.Op == "delete"}

					if !remoteWins(remoteMeta, localMeta) {
						return
					}

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
					}
				}()
			}
		}
	}()

	// Watch local
	w, err := kv.WatchAll()
	if err != nil {
		log.Fatalf("WatchAll: %v", err)
	}
	defer w.Stop()

	// Drain initial
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

	log.Printf("[%s] syncd activo: bucket=%s meta=%s clock=%s rep-stream=%s durable=%s subj=%s url=%s (clock=%d)",
		*nodeID, *bucket, metaBucket, clockBucket, *repStream, *durable, *repSubj, *natsURL, clock.Value())

	// P7/P9: announce al arrancar
	if *announceOnStart {
		announceLocalState(kv, metaKV, js, *repSubj, *bucket, *nodeID, &clock)
	}


	// Punto 9.3: reconciliación al reconectar (complementa el replay del consumer durable).
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-reconCh:
				announceLocalState(kv, metaKV, js, *repSubj, *bucket, *nodeID, &clock)
			}
		}
	}()
	// P9.4: reconcile periódico
	if *reconcileEvery > 0 {
		t := time.NewTicker(*reconcileEvery)
		defer t.Stop()
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					announceLocalState(kv, metaKV, js, *repSubj, *bucket, *nodeID, &clock)
				}
			}
		}()
	}

	// Main loop: publicar operaciones locales como mensajes duraderos
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
				jsPublishOp(js, *repSubj, op)
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
				jsPublishOp(js, *repSubj, op)
				log.Printf("[%s] publicado local: delete %s (ts=%d)", *nodeID, op.Key, op.TS)

			default:
				// Ignore
			}
		}
	}
}
