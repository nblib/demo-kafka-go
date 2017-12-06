package consumer

import (
	"testing"
	"github.com/Shopify/sarama"
	"fmt"
	"os"
	"os/signal"
	"log"
	"github.com/bsm/sarama-cluster"
)

/*
普通消费者
 */
func TestConsumer(t *testing.T) {
	//配置
	config := sarama.NewConfig()
	//接收失败通知
	config.Consumer.Return.Errors = true
	//设置使用的kafka版本,如果低于V0_10_0_0版本,消息中的timestrap没有作用.需要消费和生产同时配置
	config.Version = sarama.V0_11_0_0
	//新建一个消费者
	consumer, e := sarama.NewConsumer([]string{"10.169.0.214:9092", "10.169.0.218:9092", "10.169.0.219:9092"}, config)
	if e != nil {
		panic("error get consumer")
	}
	defer consumer.Close()

	//根据消费者获取指定的主题分区的消费者,Offset这里指定为获取最新的消息.
	partitionConsumer, err := consumer.ConsumePartition("logstash_test", 0, sarama.OffsetNewest)
	if err != nil {
		fmt.Println("error get partition consumer", err)
	}
	defer partitionConsumer.Close()
	//循环等待接受消息.

	for {
		select {
		//接收消息通道和错误通道的内容.
		case msg := <-partitionConsumer.Messages():
			fmt.Println("msg offset: ", msg.Offset, " partition: ", msg.Partition, " timestrap: ", msg.Timestamp.Format("2006-Jan-02 15:04"), " value: ", string(msg.Value))
		case err := <-partitionConsumer.Errors():
			fmt.Println(err.Err)
		}
	}
}

/**
分组的消费者
 */
func TestConsumerGroup(t *testing.T) {
	// init (custom) config, enable errors and notifications
	config := cluster.NewConfig()
	//接收失败通知
	config.Consumer.Return.Errors = true
	//默认从最新的开始消费,如果需要从头开始消费,使用这个设置
	//config.Consumer.Offsets.Initial = sarama.OffsetOldest
	// init consumer
	brokers := []string{"10.169.0.214:9092"}
	topics := []string{"logstash_test"}
	consumer, err := cluster.NewConsumer(brokers, "my-consumer-group", topics, config)

	if err != nil {
		panic(err)
	}
	defer consumer.Close()
	// 这个和消费者无关,作用为go的程序等待`ctrl+c`命令,停止程序
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// consume errors
	go func() {
		for err := range consumer.Errors() {
			log.Printf("Error: %s\n", err.Error())
		}
	}()

	// consume messages, watch signals
	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				consumer.MarkOffset(msg, "") // mark message as processed
			}
		case <-signals:
			return
		}
	}

}
