package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/montanaflynn/stats"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	brokerList  = kingpin.Flag("brokerList", "List of brokers to connect").Default("localhost:9092").String()
	topicto     = kingpin.Flag("topic", "Topic to name").Default("topic007").String()
	partition   = kingpin.Flag("partition", "Partition number").Default("0").String()
	offsetParam = kingpin.Flag("offset", "Offset").Default("0").String()
	statPeriod  = kingpin.Flag("statperiod", "Statistics period in seconds").Default("10").String()
)

func main() {
	kingpin.Parse()

	statTime, err := strconv.Atoi(*statPeriod)
	if err != nil {
		fmt.Printf("Cannot parse statistics period arg %s\n", *statPeriod)
		panic(err)
	}

	offset, err := strconv.ParseInt(*offsetParam, 10, 64)
	if err != nil {
		fmt.Printf("Cannot parse offset arg %s\n", *statPeriod)
		panic(err)
	}

	// ----- Config Consumer -----
	configC := sarama.NewConfig()
	configC.Consumer.Return.Errors = true
	configC.Version = sarama.V1_0_0_0

	brokers := strings.Split(*brokerList, ",")

	master, err := sarama.NewConsumer(brokers, configC)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	var consumer sarama.PartitionConsumer
	if offset > 0 {
		consumer, err = master.ConsumePartition(*topicto, 0, offset)
	} else {
		consumer, err = master.ConsumePartition(*topicto, 0, sarama.OffsetNewest)
	}

	if err != nil {
		panic(err)
	}

	// ----- Wait from topicto and send to topicfrom -----
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	doneCh := make(chan struct{})

	var lastOffset int64
	defer func() {
		fmt.Printf("Consumer is terminated, lastOffset=%d\n", lastOffset)
	}()

	go func() {

		var t0 int64
		var sumtime int64

		data := []float64{}

		clear := func() {
			data = data[:0]
			t0 = 0
			sumtime = 0
		}

		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)

			case msg := <-consumer.Messages():
				lastOffset = msg.Offset
				curTime := time.Now().UTC().UnixNano() / 1000

				receivedMsgTime, err := strconv.ParseInt(string(msg.Value), 10, 64)

				if err == nil {
					if receivedMsgTime == 0 {
						fmt.Println("Received a clearing message")
						clear()
						break
					}
					data = append(data, float64(curTime-receivedMsgTime)/float64(1000))
				} else {
					fmt.Printf("Cannot parse received timestamp %v !", msg.Value)
					clear()
					break
				}

				if t0 == 0 {
					t0 = curTime
				}
				sumtime += (curTime - t0)

				if sumtime >= int64(statTime)*1000000 {

					min, _ := stats.Min(data)
					med, _ := stats.Median(data)
					max, _ := stats.Max(data)

					fmt.Printf("Time in kafka topic min: %f median: %f max: %f\n", min, med, max)

					clear()
				}

				t0 = curTime

			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()
	<-doneCh
}
