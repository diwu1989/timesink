package main

import (
	"fmt"
	"log"
	"net"
	"time"

	"timesink/proto"
	"timesink/service"

	"github.com/tecbot/gorocksdb"
	"google.golang.org/grpc"
)

const grpcWorkers = 16
const port = 8000
const dbBlockSize = 32 << 10
const dbBlockCacheSize = 512 << 20
const dbBloomFilterBits = 16

func main() {
	log.Println("Starting timesink service on port", port)

	listener, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		log.Fatalln(err)
	}
	defer listener.Close()

	db, err := openRocksDB("/tmp/rocksdb")
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	grpcServer := grpc.NewServer(grpc.NumStreamWorkers(grpcWorkers))
	service := service.NewTimeSinkService(db)
	proto.RegisterTimeSinkServer(grpcServer, &service)

	log.Println("Server started at", time.Now().Unix())
	log.Fatalln(grpcServer.Serve(listener))
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
