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
const payloadSize = 256

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
	go printPeriodicReaderOffset(&reader)

	cleaner := service.NewTimeSinkCleaner(db, &reader, 5*time.Minute, nil)
	ctx, cancelCleaner := context.WithCancel(context.Background())
	go cleaner.Start(ctx)
	defer cancelCleaner()
	go printPeriodicCleanerOffset(&cleaner)

	grpcServer := grpc.NewServer(grpc.NumStreamWorkers(grpcWorkers))
	instance := service.NewTimeSinkService(db)
	proto.RegisterTimeSinkServer(grpcServer, &instance)

	// Generate 100k events per second
	reqsPerSecond := 100000
	rateLimiter := rate.NewLimiter(rate.Every(time.Second/time.Duration(reqsPerSecond)), 4*reqsPerSecond)
	// Generate events up to 1 hour into the future
	jitterDuration := time.Hour
	go generateRandomEvent(&instance, rateLimiter, jitterDuration)

	now := time.Now()
	log.Println("Server started at", now.Format(time.UnixDate))
	log.Println("Current unix timestamp", now.Unix())
	log.Fatalln(grpcServer.Serve(listener))
}

func generateRandomEvent(tss *service.TimeSinkService, rateLimiter *rate.Limiter, jitterRange time.Duration) {
	payload := make([]byte, payloadSize)
	ctx := context.Background()
	generated := 0
	for {
		r := rateLimiter.Reserve()
		if !r.OK() {
			time.Sleep(time.Millisecond)
			continue
		}
		time.Sleep(r.Delay())
		var reply *proto.QueueEventReply
		var event *proto.QueueEventRequest
		now := time.Now()
		jitter := time.Duration(float64(jitterRange) * (0.00000001 + rand.Float64()))
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
		generated++
		if generated%(100000) == 0 {
			log.Println(
				"Generated", generated,
				"EventTime", event.DeliveryTimestamp,
				"Last CancellationToken", reply.CancellationToken)
		}
	}
}

func printPeriodicCleanerOffset(cleaner *service.TimeSinkCleaner) {
	for {
		time.Sleep(10 * time.Second)
		offset := cleaner.Offset()
		eventTime := uint64(0)
		if len(offset) >= 8 {
			eventTime = binary.BigEndian.Uint64(offset)
		}
		log.Println("Cleaner offset", cleaner.Offset(), "EventTime", eventTime)
	}
}

func printPeriodicReaderOffset(reader *service.TimeSinkReader) {
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
		log.Println(
			"Reader rate/s", rate,
			"Lag", uint64(lastTime)-eventTime,
			"EventTime", eventTime,
			"Counter", reader.Counter(),
			"Offset", reader.Offset())
	}
}

func validateChannelEvents(events chan *proto.QueueEventRequest) {
	consumedEvents := 0
	for {
		event := <-events
		// invariant is that the event's time should always be less than the current unix time
		if event.DeliveryTimestamp > time.Now().Unix() {
			log.Fatalln("Invalid event from the future", event)
		}
		if len(event.Payload) != payloadSize {
			log.Fatalln("Invalid event payload", event.Payload)
		}
		consumedEvents += 1
		if consumedEvents%1000000 == 0 {
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
