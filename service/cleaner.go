package service

import (
	"context"
	"encoding/binary"
	"log"
	"time"

	"github.com/tecbot/gorocksdb"
)

const cleanerSleepS = 10

type TimeSinkCleaner struct {
	reader  *TimeSinkReader
	db      *gorocksdb.DB
	wo      *gorocksdb.WriteOptions
	offset  []byte
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
		offset:  offset,
		lag:     lag,
	}
}

func (tsc *TimeSinkCleaner) Offset() []byte {
	return tsc.offset
}

func (tsc *TimeSinkCleaner) Start(ctx context.Context) {
	deleteRangeStart := make([]byte, 0)
	for {
		// default to safe barrier unix epoch start
		timeBarrierUnix := uint64(time.Unix(0, 0).Unix())
		readerOffset := tsc.reader.Offset()
		if len(readerOffset) >= 8 {
			readerUnix := binary.BigEndian.Uint64(readerOffset)
			timeBarrierUnix = uint64(time.Unix(int64(readerUnix), 0).Add(-tsc.lag).Unix())
		}
		timeBarrierKey := make([]byte, 8)
		binary.BigEndian.PutUint64(timeBarrierKey, timeBarrierUnix)

		writeBatch := gorocksdb.NewWriteBatch()
		writeBatch.DeleteRange(deleteRangeStart, timeBarrierKey)
		err := tsc.db.Write(tsc.wo, writeBatch)
		if err != nil {
			log.Fatalln("Failed to write batch", err)
		}
		writeBatch.Clear()
		tsc.offset = timeBarrierKey
		deleteRangeStart = timeBarrierKey

		select {
		case <-time.After(cleanerSleepS * time.Second):
			// Sleep a little bit before checking again
			continue
		case <-ctx.Done():
			log.Println("TimeSinkCleaner cancelled at offset", tsc.offset)
			return
		}
	}
}
