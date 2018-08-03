package main

import (
	"fmt"
	"os"
	"os/signal"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/Shopify/sarama"
)

var (
	brokerList = kingpin.Flag("brokerList", "List of brokers to connect").Default("localhost:9092").Strings()
	topicto    = kingpin.Flag("topicto", "Topic to name").Default("topicto").String()
	topicfrom  = kingpin.Flag("topicfrom", "Topic from name").Default("topicfrom").String()
	partition  = kingpin.Flag("partition", "Partition number").Default("0").String()
	offsetType = kingpin.Flag("offsetType", "Offset Type (OffsetNewest | OffsetOldest)").Default("-1").Int()
)

func main() {
	kingpin.Parse()

	// ----- Config Consumer -----
	configC := sarama.NewConfig()
	configC.Consumer.Return.Errors = true
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

	// ----- Config Producer -----
	configP := sarama.NewConfig()
	configP.Producer.RequiredAcks = sarama.NoResponse
	configP.Producer.Retry.Max = 5
	configP.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, configP)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

	// ----- Wait from topicto and send to topicfrom -----
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)

			case inMsg := <-consumer.Messages():

				outMsg := &sarama.ProducerMessage{
					Topic: *topicfrom,
					Value: sarama.StringEncoder(inMsg.Value),
				}
				_, _, err := producer.SendMessage(outMsg)
				if err != nil {
					panic(err)
				}

				//fmt.Printf("%v Consumer has sent message to topic: %s partition: %d offset: %d\n", time.Now(), *topicfrom, partition, offset)

			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()
	<-doneCh
}
