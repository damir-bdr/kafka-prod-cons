package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/montanaflynn/stats"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	brokerList = kingpin.Flag("brokerList", "List of brokers to connect").Default("localhost:9092").Strings()
	topicto    = kingpin.Flag("topic", "Topic to name").Default("topic007").String()
	partition  = kingpin.Flag("partition", "Partition number").Default("0").String()
	statPeriod = kingpin.Flag("statperiod", "Statistics period in seconds").Default("5").String()
	offsetType = kingpin.Flag("offsetType", "Offset Type (OffsetNewest | OffsetOldest)").Default("-1").Int()
)

func main() {
	kingpin.Parse()

	statTime, err := strconv.Atoi(*statPeriod)
	if err != nil {
		fmt.Printf("Cannot parse statistics period arg %s\n", *statPeriod)
		panic(err)
	}

	// ----- Config Consumer -----
	configC := sarama.NewConfig()
	configC.Consumer.Return.Errors = true
	configC.Version = sarama.V1_0_0_0

	brokers := *brokerList

	master, err := sarama.NewConsumer(brokers, configC)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	consumer, err := master.ConsumePartition(*topicto, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	// ----- Wait from topicto and send to topicfrom -----
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	doneCh := make(chan struct{})

	go func() {

		var t0 int64
		var sumtime int64

		data := []float64{}

		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)

			case msg := <-consumer.Messages():
				curTime := time.Now().UTC().UnixNano()

				receivedMsgTime, err := strconv.ParseInt(string(msg.Value), 10, 64)
				if err == nil {
					data = append(data, float64(curTime-receivedMsgTime)/float64(time.Millisecond))
				}

				if t0 == 0 {
					t0 = curTime
				}
				sumtime += (curTime - t0)

				if time.Duration(sumtime) >= time.Duration(statTime)*time.Duration(time.Second) {

					min, _ := stats.Min(data)
					med, _ := stats.Median(data)
					max, _ := stats.Max(data)

					fmt.Printf("Time in kafka topic min: %f median: %f max: %f\n", min, med, max)

					data = data[:0]
					t0 = 0
					sumtime = 0
				}

			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()
	<-doneCh
}
