package service

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"timesink/proto"

	"github.com/tecbot/gorocksdb"
)

type TimeSinkService struct {
	db *gorocksdb.DB
	wo *gorocksdb.WriteOptions
}

func NewTimeSinkService(db *gorocksdb.DB) TimeSinkService {
	return TimeSinkService{db: db, wo: gorocksdb.NewDefaultWriteOptions()}
}

func encodeKey(timestamp int64, eventId string) []byte {
	tsBinary := make([]byte, 8)
	// Remember to use big endian so that the timestamps are sortable
	binary.BigEndian.PutUint64(tsBinary, uint64(timestamp))
	return append(tsBinary, []byte(eventId)...)
}

func (tss *TimeSinkService) QueueEvent(
	ctx context.Context,
	request *proto.QueueEventRequest,
) (*proto.QueueEventReply, error) {
	// make a bytes key out of the request timestamp and the id
	key := encodeKey(request.DeliveryTimestamp, request.Id)
	err := tss.db.Put(tss.wo, key, request.Payload)
	if err != nil {
		return nil, err
	}
	reply := proto.QueueEventReply{CancellationToken: hex.EncodeToString(key)}
	return &reply, nil
}

func (tss *TimeSinkService) CancelEvent(
	ctx context.Context,
	request *proto.CancelEventRequest,
) (*proto.CancelEventReply, error) {
	key, err := hex.DecodeString(request.CancellationToken)
	if err != nil {
		return nil, err
	}
	err = tss.db.Delete(tss.wo, key)
	if err != nil {
		return nil, err
	}
	reply := proto.CancelEventReply{}
	return &reply, nil
}
