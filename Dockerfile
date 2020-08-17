FROM golang:1.15.0-buster
COPY . /src
RUN cd /src && \
	apt update && \
	apt install librocksdb-dev -y && \
	go build && \
	apt clean && \
	rm -rf /var/lib/apt/lists/* && \
	cp timesink /timesink && \
	rm -rf /src /go
EXPOSE 8000
CMD ["/timesink"]
