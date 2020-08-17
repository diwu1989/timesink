package service

import (
	"context"
	"encoding/binary"
	"log"
	"time"

	"github.com/tecbot/gorocksdb"
)

const cleanerSleepMs = 100

type TimeSinkCleaner struct {
	reader  *TimeSinkReader
	db      *gorocksdb.DB
	wo      *gorocksdb.WriteOptions
	ro      *gorocksdb.ReadOptions
	offset  []byte
	counter uint64
	lag     time.Duration
}

func NewTimeSinkCleaner(db *gorocksdb.DB, reader *TimeSinkReader, lag time.Duration, offset []byte) TimeSinkCleaner {
	if offset == nil {
		offset = make([]byte, 0)
	}
	if lag < 0 {
		// Must be running with a positive lag, default to 5 minute behind the reader
		lag = time.Minute * 5
	}
	return TimeSinkCleaner{
		reader:  reader,
		db:      db,
		wo:      gorocksdb.NewDefaultWriteOptions(),
		ro:      gorocksdb.NewDefaultReadOptions(),
		offset:  offset,
		counter: 0,
		lag:     lag,
	}
}

func (tsc *TimeSinkCleaner) Offset() []byte {
	return tsc.offset
}

func (tsc *TimeSinkCleaner) Counter() uint64 {
	return tsc.counter
}

func (tsc *TimeSinkCleaner) Start(ctx context.Context) {
	for {
		itr := tsc.db.NewIterator(tsc.ro)
		itr.Seek(tsc.offset)

		// default to safe barrier unix epoch start
		timeBarrierUnix := uint64(time.Unix(0, 0).Unix())
		readerOffset := tsc.reader.Offset()
		if len(readerOffset) >= 8 {
			readerUnix := binary.BigEndian.Uint64(readerOffset)
			timeBarrierUnix = uint64(time.Unix(int64(readerUnix), 0).Add(-tsc.lag).Unix())
		}
		writeBatch := gorocksdb.NewWriteBatch()
		var lastBatchkey []byte
		for ; itr.Valid(); itr.Next() {
			keyBytes := make([]byte, itr.Key().Size())
			if copy(keyBytes, itr.Key().Data()) != itr.Key().Size() {
				log.Fatalln("Failed to copy key", itr.Key().Data())
			}
			if len(keyBytes) < 8 {
				log.Fatalln("Invalid key", keyBytes)
			}
			eventTime := binary.BigEndian.Uint64(keyBytes)
			if eventTime > timeBarrierUnix {
				// do not prune past the time barrier
				break
			}
			writeBatch.Delete(keyBytes)
			lastBatchkey = keyBytes
			if writeBatch.Count() > 1000 {
				err := tsc.db.Write(tsc.wo, writeBatch)
				if err != nil {
					log.Fatalln("Failed to write batch", err)
				}
				tsc.offset = lastBatchkey
				tsc.counter += uint64(writeBatch.Count())
				writeBatch.Clear()
			}
		}
		if writeBatch.Count() > 0 {
			err := tsc.db.Write(tsc.wo, writeBatch)
			if err != nil {
				log.Fatalln("Failed to write batch", err)
			}
			tsc.offset = lastBatchkey
			tsc.counter += uint64(writeBatch.Count())
			writeBatch.Clear()
		}
		itr.Close()
		select {
		case <-time.After(cleanerSleepMs * time.Millisecond):
			// Sleep a little bit before checking again
			continue
		case <-ctx.Done():
			log.Println("TimeSinkCleaner cancelled at offset", tsc.offset)
			return
		}
	}
}
