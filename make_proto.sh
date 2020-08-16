#!/bin/bash
protoc --proto_path=proto --go_out=plugins=grpc:proto --go_opt=paths=source_relative proto/service.proto
