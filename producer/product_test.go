package producer

import (
	"github.com/Shopify/sarama"
	"fmt"
	"testing"
)

/*
测试异步生产者
 */
func TestAsyn_Product(t *testing.T) {
	//设置配置
	config := sarama.NewConfig()
	//等待服务器所有副本都保存成功后的响应
	config.Producer.RequiredAcks = sarama.WaitForAll
	//随机的分区类型
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	//是否等待成功后的响应
	config.Producer.Return.Successes = true

	//使用配置,新建一个异步生产者
	producer, e := sarama.NewAsyncProducer([]string{"192.168.233.143:9092"}, config)
	if e != nil {
		panic(e)
	}
	defer producer.AsyncClose()

	//发送的消息,主题,key
	msg := &sarama.ProducerMessage{
		Topic: "test-demo",
		Key:   sarama.StringEncoder("test"),
	}

	var value string
	for {
		value = "this is a message"
		//设置发送的真正内容
		msg.Value = sarama.ByteEncoder(value)
		fmt.Println(value)

		//使用通道发送
		producer.Input() <- msg

		//循环判断哪个通道发送过来数据.
		select {
		case suc := <-producer.Successes():
			fmt.Println("offset: ", suc.Offset, "timestamp: ", suc.Timestamp.String(), "partitions: ", suc.Partition)
			return
		case fail := <-producer.Errors():
			fmt.Println("err: ", fail.Err)
		}
	}
}
