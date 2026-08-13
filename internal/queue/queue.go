package queue

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"forge/internal/job"
)

type Config struct {
	BaseBackoff  time.Duration
	MaxBackoff   time.Duration
	MaxReady     int
	PollInterval time.Duration
}

type Queue struct {
	rdb          *redis.Client
	name         string
	baseBackoff  time.Duration
	maxBackoff   time.Duration
	maxReady     int
	pollInterval time.Duration
}

func New(rdb *redis.Client, name string, cfg Config) *Queue {
	return &Queue{
		rdb:          rdb,
		name:         name,
		baseBackoff:  cfg.BaseBackoff,
		maxBackoff:   cfg.MaxBackoff,
		maxReady:     cfg.MaxReady,
		pollInterval: cfg.PollInterval,
	}
}

func (q *Queue) key(suffix string) string {
	return "forge:" + q.name + ":" + suffix
}

func (q *Queue) Enqueue(ctx context.Context, payload json.RawMessage, maxAttempts int) (*job.Job, error) {
	for {

		n, err := q.rdb.LLen(ctx, q.key("ready")).Result()
		if err != nil {
			return nil, err
		}
		if n < int64(q.maxReady) {
			break
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(q.pollInterval):
		}
	}

	j := job.New(payload, maxAttempts)
	data, err := j.Marshal()
	if err != nil {
		return nil, err
	}

	if err := q.rdb.LPush(ctx, q.key("ready"), data).Err(); err != nil {
		return nil, err
	}
	return j, nil
}

func (q *Queue) Dequeue(ctx context.Context, timeout time.Duration) (*job.Job, error) {

	data, err := q.rdb.BLMove(ctx, q.key("ready"), q.key("processing"), "RIGHT", "LEFT", timeout).Result()
	if err == redis.Nil {

		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return job.Unmarshal([]byte(data))
}

func (q *Queue) Ack(ctx context.Context, j *job.Job) error {
	data, err := j.Marshal()
	if err != nil {
		return err
	}

	return q.rdb.LRem(ctx, q.key("processing"), 1, data).Err()
}

func (q *Queue) Fail(ctx context.Context, j *job.Job, cause error) (deadLettered bool, err error) {

	oldData, err := j.Marshal()
	if err != nil {
		return false, err
	}

	j.Attempts++
	if cause != nil {
		j.LastError = cause.Error()
	}

	newData, err := j.Marshal()
	if err != nil {
		return false, err
	}

	pipe := q.rdb.TxPipeline()
	pipe.LRem(ctx, q.key("processing"), 1, oldData)

	if j.Attempts >= j.MaxAttempts {

		pipe.LPush(ctx, q.key("dead"), newData)
		deadLettered = true
	} else {

		delay := q.baseBackoff * time.Duration(1<<uint(j.Attempts))
		if delay > q.maxBackoff {
			delay = q.maxBackoff
		}
		dueAtMillis := time.Now().Add(delay).UnixMilli()
		pipe.ZAdd(ctx, q.key("delayed"), redis.Z{Score: float64(dueAtMillis), Member: newData})
	}

	_, err = pipe.Exec(ctx)
	return deadLettered, err
}

func (q *Queue) PromoteDelayed(ctx context.Context) (int, error) {
	nowMillis := time.Now().UnixMilli()

	due, err := q.rdb.ZRangeByScore(ctx, q.key("delayed"), &redis.ZRangeBy{
		Min: "-inf",
		Max: strconv.FormatInt(nowMillis, 10),
	}).Result()
	if err != nil {
		return 0, err
	}

	for _, data := range due {
		pipe := q.rdb.TxPipeline()
		pipe.ZRem(ctx, q.key("delayed"), data)
		pipe.LPush(ctx, q.key("ready"), data)
		if _, err := pipe.Exec(ctx); err != nil {
			return 0, err
		}
	}
	return len(due), nil
}

func (q *Queue) RunPoller(ctx context.Context) {
	ticker := time.NewTicker(q.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := q.PromoteDelayed(ctx); err != nil {
				log.Printf("poller: promote delayed: %v", err)
			}
		}
	}
}
