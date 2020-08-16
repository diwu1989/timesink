package service

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/tecbot/gorocksdb"
	"log"
	"time"
	"timesink/proto"
)

const readerSleepMs = 100

type TimeSinkReader struct {
	db      *gorocksdb.DB
	wo      *gorocksdb.WriteOptions
	ro      *gorocksdb.ReadOptions
	output  chan *proto.QueueEventRequest
	offset  []byte
	counter uint64
}

func NewTimeSinkReader(db *gorocksdb.DB, output chan *proto.QueueEventRequest, offset []byte) TimeSinkReader {
	if offset == nil {
		offset = make([]byte, 0)
	}
	return TimeSinkReader{
		db:      db,
		wo:      gorocksdb.NewDefaultWriteOptions(),
		ro:      gorocksdb.NewDefaultReadOptions(),
		output:  output,
		offset:  offset,
		counter: 0,
	}
}

func (tsr *TimeSinkReader) Offset() []byte {
	return tsr.offset
}

func (tsr *TimeSinkReader) Counter() uint64 {
	return tsr.counter
}

func (tsr *TimeSinkReader) Start(ctx context.Context) {
	for {
		// loop forever reading from rocksdb
		itr := tsr.db.NewIterator(tsr.ro)
		itr.Seek(tsr.offset)

		if itr.Valid() && bytes.Compare(itr.Key().Data(), tsr.offset) == 0 {
			// already consumed this key, continue
			itr.Next()
		}

		unixNow := uint64(time.Now().Unix())

		for ; itr.Valid(); itr.Next() {
			keyBytes := make([]byte, itr.Key().Size())
			if copy(keyBytes, itr.Key().Data()) != itr.Key().Size() {
				log.Fatalln("Failed to copy key", itr.Key().Data())
			}
			itr.Key().Free()
			if len(keyBytes) < 8 {
				log.Fatalln("Invalid key", keyBytes)
			}
			// first 8 bytes are the unix timestamp
			eventTime := binary.BigEndian.Uint64(keyBytes)
			if eventTime > unixNow {
				// do not process events in the future, entry at position is not consumed
				break
			}
			// event is valid for processing
			payload := make([]byte, itr.Value().Size())
			itr.Value().Free()
			if copy(payload, itr.Value().Data()) != itr.Value().Size() {
				log.Fatalln("Failed to copy value", itr.Value().Data())
			}
			tsr.output <- &proto.QueueEventRequest{
				Id:                string(keyBytes[8:]),
				DeliveryTimestamp: int64(eventTime),
				Payload:           payload,
			}
			tsr.counter += 1
			tsr.offset = keyBytes
		}
		itr.Close()
		select {
		case <-time.After(readerSleepMs * time.Millisecond):
			// Sleep a little bit before checking again
			continue
		case <-ctx.Done():
			log.Println("TimeSinkReader cancelled at offset", tsr.offset)
			return
		}
	}
}
