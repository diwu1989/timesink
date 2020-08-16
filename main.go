package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	"timesink/proto"
	"timesink/service"

	"github.com/google/uuid"
	"github.com/tecbot/gorocksdb"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
)

const grpcWorkers = 16
const port = 8000
const dbBlockSize = 32 << 10
const dbBlockCacheSize = 512 << 20
const dbBloomFilterBits = 16
const dbPath = "/tmp/timesinkdb"

func main() {
	log.Println("Starting timesink service on port", port)

	listener, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		log.Fatalln(err)
	}
	defer listener.Close()

	db, err := openRocksDB(dbPath)
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()
	log.Println("RocksDB database opened", dbPath)

	eventsChannel := make(chan *proto.QueueEventRequest, 100)
	go validateChannelEvents(eventsChannel)

	reader := service.NewTimeSinkReader(db, eventsChannel, nil)

	ctx, cancelReader := context.WithCancel(context.Background())
	go reader.Start(ctx)
	defer cancelReader()
	go printPeriodicOffset(&reader)

	grpcServer := grpc.NewServer(grpc.NumStreamWorkers(grpcWorkers))
	service := service.NewTimeSinkService(db)
	proto.RegisterTimeSinkServer(grpcServer, &service)

	// Generate 100k events per second
	rateLimiter := rate.NewLimiter(rate.Every(time.Second/100000), 10000)
	go generateRandomEvent(&service, rateLimiter)

	now := time.Now()
	log.Println("Server started at", now.Format(time.UnixDate))
	log.Println("Current unix timestamp", now.Unix())
	log.Fatalln(grpcServer.Serve(listener))
}

func generateRandomEvent(tss *service.TimeSinkService, rateLimiter *rate.Limiter) {
	payload := make([]byte, 256)
	ctx := context.Background()
	generated := 0
	batchSize := 1000
	for {
		r := rateLimiter.ReserveN(time.Now(), batchSize)
		if !r.OK() {
			time.Sleep(time.Millisecond)
			continue
		}
		time.Sleep(r.Delay())
		var reply *proto.QueueEventReply
		var event *proto.QueueEventRequest
		now := time.Now()
		for i := 0; i < batchSize; i++ {
			jitter := time.Duration(10 * float64(time.Minute) * (0.001 + rand.Float64()))
			rand.Read(payload)
			event = &proto.QueueEventRequest{
				DeliveryTimestamp: now.Add(jitter).Unix(),
				Id:                uuid.New().String(),
				Payload:           payload,
			}
			replyQ, err := tss.QueueEvent(ctx, event)
			reply = replyQ
			if err != nil {
				log.Fatalln(err)
			}
		}
		generated += batchSize
		if generated%100000 == 0 {
			log.Println(
				"Generated", generated,
				"EventTime", event.DeliveryTimestamp,
				"Last CancellationToken", reply.CancellationToken)
		}
	}
}

func printPeriodicOffset(reader *service.TimeSinkReader) {
	last := uint64(0)
	lastTime := time.Now().Unix()
	for {
		time.Sleep(5 * time.Second)
		offset := reader.Offset()
		eventTime := uint64(0)
		if len(offset) >= 8 {
			eventTime = binary.BigEndian.Uint64(offset)
		}
		counter := reader.Counter()
		rate := float64(counter-last) / float64(time.Now().Unix()-lastTime)
		last = counter
		lastTime = time.Now().Unix()
		log.Println("Reader rate/s", rate, "Lag", uint64(lastTime) - eventTime, "EventTime", eventTime, "Counter", reader.Counter(), "Offset", reader.Offset())
	}
}

func validateChannelEvents(events chan *proto.QueueEventRequest) {
	consumedEvents := 0
	for {
		event := <-events
		// invariant is that the event's time should always be less than the current unix time
		now := time.Now()
		if event.DeliveryTimestamp > now.Unix() {
			log.Fatalln("Invalid event from the future", event)
		}
		consumedEvents += 1
		if consumedEvents%100000 == 0 {
			// every 1k events, print it to the log
			log.Println(
				"Consumed event", consumedEvents,
				"ts", event.DeliveryTimestamp,
				"id", event.Id,
				"payload length", len(event.Payload))
		}
	}
}

func openRocksDB(path string) (*gorocksdb.DB, error) {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(dbBlockCacheSize))
	bbto.SetFilterPolicy(gorocksdb.NewBloomFilter(dbBloomFilterBits))
	bbto.SetBlockSize(dbBlockSize)

	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)

	return gorocksdb.OpenDb(opts, path)
}
