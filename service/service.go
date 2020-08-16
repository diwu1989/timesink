package service

import (
	"context"
	"timesink/proto"

	"github.com/tecbot/gorocksdb"
	"google.golang.org/grpc/status"
)

type TimeSinkService struct {
	db *gorocksdb.DB
}

func NewTimeSinkService(db *gorocksdb.DB) TimeSinkService {
	return TimeSinkService{db: db}
}

func (*TimeSinkService) QueueEvent(
	ctx context.Context,
	request *proto.QueueEventRequest,
) (*proto.QueueEventReply, error) {
	return nil, status.Error(1, "error")
}

func (*TimeSinkService) CancelEvent(
	ctx context.Context,
	request *proto.CancelEventRequest,
) (*proto.CancelEventReply, error) {
	return nil, status.Error(1, "error")
}
