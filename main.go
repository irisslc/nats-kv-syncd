package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
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
		// Si el JSON está corrupto, actuamos como si no hubiese meta.
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

func main() {
	natsURL := flag.String("nats-url", "nats://localhost:4222", "NATS URL")
	bucket := flag.String("bucket", "config", "KV bucket")
	nodeID := flag.String("node-id", "", "Node ID")
	repSubj := flag.String("rep-subj", "rep.kv.ops", "Replication subject")
	flag.Parse()

	if *nodeID == "" {
		log.Fatal("Falta --node-id")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	nc, err := nats.Connect(*natsURL)
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
		// Opcional (pero útil): si no hay meta aún para una clave existente, inicialízala con el "Created" local.
		// Esto está dentro del punto 6 (almacenamiento de metadatos) y ayuda en el primer arranque.
		if _, ok := loadMeta(metaKV, e.Key()); !ok {
			saveMeta(metaKV, e.Key(), Meta{TS: e.Created().Unix(), NodeID: "", Deleted: e.Operation() != nats.KeyValuePut})
		}
	}

	log.Printf("[%s] syncd activo: bucket=%s meta=%s subj=%s url=%s", *nodeID, *bucket, metaBucket, *repSubj, *natsURL)

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

			now := time.Now().Unix()

			switch e.Operation() {
			case nats.KeyValuePut:
				op := Operation{
					Op:     "put",
					Bucket: *bucket,
					Key:    e.Key(),
					Value:  string(e.Value()),
					TS:     now,
					NodeID: *nodeID,
				}
				saveMeta(metaKV, op.Key, Meta{TS: op.TS, NodeID: op.NodeID, Deleted: false})

				b, _ := json.Marshal(op)
				if err := nc.Publish(*repSubj, b); err != nil {
					log.Printf("[%s] publish put %s: %v", *nodeID, op.Key, err)
				} else {
					log.Printf("[%s] publicado local: put %s (ts=%d)", *nodeID, op.Key, op.TS)
				}

			case nats.KeyValueDelete, nats.KeyValuePurge:
				op := Operation{
					Op:     "delete",
					Bucket: *bucket,
					Key:    e.Key(),
					TS:     now,
					NodeID: *nodeID,
				}
				saveMeta(metaKV, op.Key, Meta{TS: op.TS, NodeID: op.NodeID, Deleted: true})

				b, _ := json.Marshal(op)
				if err := nc.Publish(*repSubj, b); err != nil {
					log.Printf("[%s] publish delete %s: %v", *nodeID, op.Key, err)
				} else {
					log.Printf("[%s] publicado local: delete %s (ts=%d)", *nodeID, op.Key, op.TS)
				}

			default:
				// Ignore other internal ops (e.g. initialization markers)
			}
		}
	}
}
