package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	periodParam = kingpin.Flag("ticksperiod", "Time between ticks in microseconds").Default("500000").String()
	brokerList  = kingpin.Flag("brokerList", "List of brokers to connect").Default("localhost:9092").String()
	topicto     = kingpin.Flag("topic", "Topic to name").Default("topic007").String()
)

func main() {
	kingpin.Parse()

	d, err := strconv.Atoi(*periodParam)
	if err != nil {
		fmt.Printf("Cannot parse period arg %s\n", *periodParam)
		panic(err)
	}

	brokers := strings.Split(*brokerList, ",")

	configP := sarama.NewConfig()
	configP.Producer.RequiredAcks = sarama.NoResponse
	configP.Producer.Retry.Max = 5
	configP.Producer.Return.Successes = true
	configP.Version = sarama.V1_0_0_0

	producer, err := sarama.NewSyncProducer(brokers, configP)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

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

				msg := &sarama.ProducerMessage{
					Topic: *topicto,
					Value: sarama.StringEncoder(timestamp),
				}
				_, _, err := producer.SendMessage(msg)
				if err != nil {
					panic(err)
				}

				//fmt.Printf("%v Message is sent to topic: %s partition: %d offset: %d\n", t, *topicto, partition, offset)

			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	ticker.Stop()
}
