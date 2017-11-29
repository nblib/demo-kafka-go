package main

import (
	"github.com/Shopify/sarama"
	"fmt"
)

func main() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.Return.Successes = true
	producer, e := sarama.NewAsyncProducer([]string{"192.168.233.143:9092"}, config)
	if e != nil {
		panic(e)
	}
	defer producer.AsyncClose()
	msg := &sarama.ProducerMessage{
		Topic:     "test-demo",
		Partition: int32(2),
		Key:       sarama.StringEncoder("key"),
	}

	var value string
	for {
		fmt.Scanln(&value)
		msg.Value = sarama.ByteEncoder(value)
		fmt.Println(value)

		producer.Input() <- msg

		select {
		case suc := <- producer.Successes():
			fmt.Println("offset: ",suc.Offset,"timestamp: ",suc.Timestamp.String(),"partitions: ", suc.Partition)
		}
	}
}
