package main

import (
	"fmt"
	"log"
	"runtime"
	"strconv"
	"time"

	"github.com/montanaflynn/stats"
	nats "github.com/nats-io/go-nats"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	broker     = kingpin.Flag("broker", "List of brokers to connect").Default(nats.DefaultURL).String()
	topicto    = kingpin.Flag("topic", "Topic to name").Default("topic007").String()
	statPeriod = kingpin.Flag("statperiod", "Statistics period in seconds").Default("10").String()
)

func main() {
	kingpin.Parse()

	statTime, err := strconv.Atoi(*statPeriod)
	if err != nil {
		fmt.Printf("Cannot parse statistics period arg %s\n", *statPeriod)
		panic(err)
	}

	// ----- Config Consumer -----
	nc, err := nats.Connect(*broker)
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}
	// ---------------------------

	var t0 int64
	var sumtime int64
	data := []float64{}

	clear := func() {
		data = data[:0]
		t0 = 0
		sumtime = 0
	}

	nc.Subscribe(*topicto, func(msg *nats.Msg) {
		curTime := time.Now().UTC().UnixNano() / 1000
		receivedMsgTime, err := strconv.ParseInt(string(msg.Data), 10, 64)

		if err == nil {
			if receivedMsgTime == 0 {
				fmt.Println("Received a clearing message")
				clear()
				return
			}
			data = append(data, float64(curTime-receivedMsgTime)/float64(1000))
		} else {
			fmt.Printf("Cannot parse received timestamp %v !", msg.Data)
			clear()
			return
		}

		if t0 == 0 {
			t0 = curTime
		}
		sumtime += (curTime - t0)

		if sumtime >= int64(statTime)*1000000 {

			min, _ := stats.Min(data)
			med, _ := stats.Median(data)
			max, _ := stats.Max(data)
			standartDeviat, _ := stats.StandardDeviation(data)

			fmt.Printf("Time spent in nats min: %f median: %f max: %f devitation: %f amount data: %d \n", min, med, max, standartDeviat, len(data))

			clear()
		}

		t0 = curTime

	})

	nc.Flush()

	if err := nc.LastError(); err != nil {
		log.Fatal(err)
	}

	log.Printf("Listening on [%s]\n", *topicto)

	runtime.Goexit()
}
