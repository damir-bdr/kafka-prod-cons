package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/nats-io/go-nats"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	periodParam = kingpin.Flag("ticksperiod", "Time between ticks in microseconds").Default("500000").String()
	broker      = kingpin.Flag("broker", "The nat server URL").Default(nats.DefaultURL).String()
	topicto     = kingpin.Flag("topic", "Topic to name").Default("topic007").String()
)

func main() {
	kingpin.Parse()

	d, err := strconv.Atoi(*periodParam)
	if err != nil {
		fmt.Printf("Cannot parse period arg %s\n", *periodParam)
		panic(err)
	}

	nc, err := nats.Connect(*broker)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	doneCh := make(chan struct{})

	ticker := time.NewTicker(time.Duration(d) * time.Microsecond)

	go func() {

		firstTime := 0

		for {
			select {
			case <-ticker.C:

				var timestamp string

				if firstTime == 0 {
					firstTime = 1
					timestamp = strconv.FormatInt(0, 10)
				} else {
					timestamp = strconv.FormatInt(time.Now().UTC().UnixNano()/1000, 10)
				}

				nc.Publish(*topicto, []byte(timestamp))
				nc.Flush()

				if err := nc.LastError(); err != nil {
					log.Fatal(err)
				} else {
					log.Printf("Published [%s] : '%s'\n", *topicto, timestamp)
				}

			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	ticker.Stop()
}
