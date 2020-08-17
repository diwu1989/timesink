FROM golang:1.15.0-buster
COPY . /src
RUN cd /src && \
	apt update && \
	apt install librocksdb5.17 librocksdb-dev -y && \
	go build && \
	apt purge librocksdb-dev -y && \
	apt autoremove -y && \
	apt clean && \
	rm -rf /var/lib/apt/lists/* && \
	rm -rf ~/.cache && \
	cp timesink /timesink && \
	rm -rf /src /go
EXPOSE 8000
CMD ["/timesink"]
