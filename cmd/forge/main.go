package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/smtp"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"forge/internal/queue"
	"forge/internal/worker"
)

const mailhogAddr = "localhost:1025"

func main() {
	cfg := loadConfig()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.redisAddr})
	defer rdb.Close()

	q := queue.New(rdb, cfg.queueName, queue.Config{
		BaseBackoff:  cfg.baseBackoff,
		MaxBackoff:   cfg.maxBackoff,
		MaxReady:     cfg.maxReady,
		PollInterval: cfg.pollInterval,
	})

	pool := worker.New(q, cfg.concurrency, demoHandler)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("shutdown signal received, waiting for in-flight jobs to finish...")
		cancel()
	}()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		q.RunPoller(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		pool.Run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runProducer(ctx, q)
	}()

	wg.Wait()
	log.Println("forge stopped")
}

type emailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func runProducer(ctx context.Context, q *queue.Queue) {
	const demoJobCount = 20

	for i := 1; i <= demoJobCount; i++ {
		if ctx.Err() != nil {
			return
		}

		payload, err := json.Marshal(emailPayload{
			To:      fmt.Sprintf("user%d@example.com", i),
			Subject: fmt.Sprintf("Demo email #%d", i),
			Body:    fmt.Sprintf("This is demo job number %d from Forge.", i),
		})
		if err != nil {
			log.Printf("producer: marshal failed: %v", err)
			return
		}

		j, err := q.Enqueue(ctx, payload, 5)
		if err != nil {
			log.Printf("producer: enqueue failed: %v", err)
			return
		}
		log.Printf("enqueued job %s (n=%d)", j.ID, i)

		select {
		case <-ctx.Done():
			return
		case <-time.After(300 * time.Millisecond):
		}
	}
	log.Println("producer: done enqueuing demo jobs")
}

func demoHandler(ctx context.Context, payload []byte) error {
	var email emailPayload
	if err := json.Unmarshal(payload, &email); err != nil {
		return fmt.Errorf("invalid email payload: %w", err)
	}

	time.Sleep(time.Second + time.Duration(rand.Intn(2000))*time.Millisecond)

	if rand.Intn(4) == 0 {
		return fmt.Errorf("simulated provider failure sending to %s", email.To)
	}

	msg := []byte(fmt.Sprintf(
		"From: forge-demo@example.com\r\nTo: %s\r\nSubject: %s\r\n\r\n%s\r\n",
		email.To, email.Subject, email.Body,
	))

	return smtp.SendMail(mailhogAddr, nil, "forge-demo@example.com", []string{email.To}, msg)
}

type config struct {
	redisAddr    string
	queueName    string
	concurrency  int
	maxReady     int
	baseBackoff  time.Duration
	maxBackoff   time.Duration
	pollInterval time.Duration
}

func loadConfig() config {
	return config{
		redisAddr:   getEnv("REDIS_ADDR", "localhost:6379"),
		queueName:   getEnv("QUEUE_NAME", "default"),
		concurrency: getEnvInt("CONCURRENCY", 10),
		maxReady:    getEnvInt("MAX_READY", 1000),

		baseBackoff:  time.Duration(getEnvInt("BASE_BACKOFF_MS", 500)) * time.Millisecond,
		maxBackoff:   time.Duration(getEnvInt("MAX_BACKOFF_MS", 30000)) * time.Millisecond,
		pollInterval: time.Duration(getEnvInt("POLL_INTERVAL_MS", 500)) * time.Millisecond,
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("invalid value %q for %s, using default %d", v, key, fallback)
		return fallback
	}
	return n
}
