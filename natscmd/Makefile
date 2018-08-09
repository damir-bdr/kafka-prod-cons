GOCMD=go
GOBUILD=$(GOCMD) build
GOGET=$(GOCMD) get

all: build
build:
	$(GOBUILD) -o sub consumer/sub.go
	$(GOBUILD) -o pub producer/pub.go

clean:
	rm -f sub
	rm -f pub

deps:
	$(GOGET) github.com/nats-io/go-nats
	$(GOGET) github.com/montanaflynn/stats
	$(GOGET) gopkg.in/alecthomas/kingpin.v2 


