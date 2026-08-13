package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"forge/internal/job"
	"forge/internal/queue"
)

type Handler func(ctx context.Context, payload []byte) error

type Pool struct {
	q           *queue.Queue
	handler     Handler
	concurrency int
}

func New(q *queue.Queue, concurrency int, handler Handler) *Pool {
	return &Pool{q: q, handler: handler, concurrency: concurrency}
}

func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < p.concurrency; i++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()
			p.loop(ctx, workerID)
		}(i)
	}
	wg.Wait()
}

func (p *Pool) loop(ctx context.Context, workerID int) {
	for {

		if ctx.Err() != nil {
			return
		}

		j, err := p.q.Dequeue(ctx, 2*time.Second)
		if err != nil {
			log.Printf("worker %d: dequeue error: %v", workerID, err)
			continue
		}
		if j == nil {
			continue
		}

		p.process(ctx, workerID, j)
	}
}

func (p *Pool) process(ctx context.Context, workerID int, j *job.Job) {
	log.Printf("worker %d: started job %s", workerID, j.ID)

	handlerErr := p.runHandler(ctx, j)

	finishCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if handlerErr != nil {

		deadLettered, failErr := p.q.Fail(finishCtx, j, handlerErr)
		if failErr != nil {

			log.Printf("worker %d: job %s failed (%v) and recording that failure also failed: %v", workerID, j.ID, handlerErr, failErr)
			return
		}
		if deadLettered {
			log.Printf("worker %d: job %s dead-lettered after %d/%d attempts: %v", workerID, j.ID, j.Attempts, j.MaxAttempts, handlerErr)
		} else {
			log.Printf("worker %d: job %s failed, scheduled for retry (attempt %d/%d): %v", workerID, j.ID, j.Attempts, j.MaxAttempts, handlerErr)
		}
		return
	}
	if err := p.q.Ack(finishCtx, j); err != nil {
		log.Printf("worker %d: job %s ack failed: %v", workerID, j.ID, err)
		return
	}
	log.Printf("worker %d: finished job %s", workerID, j.ID)
}

func (p *Pool) runHandler(ctx context.Context, j *job.Job) (err error) {

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return p.handler(ctx, j.Payload)
}
