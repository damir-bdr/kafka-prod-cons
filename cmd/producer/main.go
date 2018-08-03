package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	periodParam = kingpin.Flag("period", "Time between ticks in microseconds").Default("500000").String()
	//	brokerList  = kingpin.Flag("brokerList", "List of brokers to connect").Default("localhost:9092").Strings()
	brokerList = kingpin.Flag("brokerList", "List of brokers to connect").Default("206.189.77.86:9092").Strings()
	topicto    = kingpin.Flag("topicto", "Topic to name").Default("topicto").String()
	topicfrom  = kingpin.Flag("topicfrom", "Topic from name").Default("topicfrom").String()
)

func main() {
	kingpin.Parse()

	d, err := strconv.Atoi(*periodParam)
	if err != nil {
		fmt.Printf("Cannot parse period arg %s\n", *periodParam)
		panic(err)
	}

	//brokers := *brokerList
	brokers := []string{"206.189.77.86:9092"}

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

	// ----- Config Consumer -----
	configC := sarama.NewConfig()
	configC.Consumer.Return.Errors = true
	configC.Version = sarama.V1_0_0_0

	master, err := sarama.NewConsumer(brokers, configC)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	consumer, err := master.ConsumePartition(*topicfrom, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	doneCh := make(chan struct{})

	ticker := time.NewTicker(time.Duration(d) * time.Microsecond)

	go func() {
		for {
			select {
			case <-ticker.C:
				timestamp := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)

				msg := &sarama.ProducerMessage{
					Topic: *topicto,
					Value: sarama.StringEncoder(timestamp),
				}
				_, _, err := producer.SendMessage(msg)
				if err != nil {
					panic(err)
				}

				//fmt.Printf("%v Message is sent to topic: %s partition: %d offset: %d\n", t, *topicto, partition, offset)

			case err := <-consumer.Errors():
				fmt.Println(err)

			case msg := <-consumer.Messages():
				curTime := time.Now().UTC().UnixNano()

				receivedTime, err := strconv.ParseInt(string(msg.Value), 10, 64)
				if err == nil {
					deltaTime := float64(curTime-receivedTime) / float64(time.Millisecond)

					fmt.Printf("deltaTime=%v\n", deltaTime)
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
