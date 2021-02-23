package nsq

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/navi-tt/xqueue"
	"github.com/nsqio/go-nsq"
	"golang.org/x/sync/errgroup"
)

type Queue struct {
	ctx      context.Context
	config   xqueue.QueueConfig
	producer *nsq.Producer
	consumer *nsq.Consumer
	message  *nsq.Message
}

func (q *Queue) init(addr string) error {
	var (
		eg, _ = errgroup.WithContext(q.ctx)
	)
	if q.consumer != nil {
		eg.Go(func() error {
			return q.consumer.ConnectToNSQD(addr)
		})
	}

	if q.producer != nil {
		//
	}
	return eg.Wait()
}

func (q *Queue) topic() string {
	return q.config.Topic
}

func (q *Queue) groupId() string {
	return q.config.GroupId
}

func (q *Queue) tags() []string {
	return q.config.Tags
}

func HandleMessage(msg *nsq.Message) error {
	fmt.Println(string(msg.Body))
	return nil
}

func (q *Queue) Enqueue(ctx context.Context, msg xqueue.Message) error {
	if q.producer == nil {
		return fmt.Errorf("producer no init")
	}

	return q.producer.Publish(q.topic(), msg.GetData())
}

func (q *Queue) Dequeue(ctx context.Context) (xqueue.Message, error) {
	//还不太懂nsq的dequeue，看范例是直接有一个handle func
	return nil, nil
}

type Message struct {
	topic     string
	groupId   string
	tags      []string
	data      []byte
	messageId string
}

func (m *Message) GetTopic() string {
	return m.topic
}

func (m *Message) SetTopic(topic string) {
	m.topic = topic
}

func (m *Message) SetTags(tags []string) {
	m.tags = tags
}

func (m *Message) GetTags() []string {
	return m.tags
}

func (m *Message) SetGroupId(gid string) {
	m.groupId = gid
}

func (m *Message) GetGroupId() string {
	return m.groupId
}

func (m *Message) SetData(data []byte) {
	m.data = data
}

func (m *Message) GetData() []byte {
	return m.data
}

func (m *Message) MessageId() string {
	return m.messageId
}

func (m *Message) DequeueCount() int64 { // 已出队消费次数
	return 0
}

// Provider redis session provider
type Provider struct {
	config Config
	queue  *Queue
}

var nsqmqpder = &Provider{}

type Config struct {
	Addr           string          `json:"addr"`
	Channel        string          `json:"channel"`
	EnableConsumer bool            `json:"enable_consumer"`
	EnableProducer bool            `json:"enable_producer"`
	HandlerFunc    nsq.HandlerFunc `json:"handler_func"`
}

func (p *Provider) QueueInit(ctx context.Context, config xqueue.QueueConfig) error {
	if p.queue == nil {
		err := json.Unmarshal([]byte(config.ProviderJsonConfig), &p.config)
		if err != nil {
			return err
		}
		var (
			cs *nsq.Consumer = nil
			pd *nsq.Producer = nil
		)

		if p.config.EnableConsumer {

			cs, err = nsq.NewConsumer(config.Topic, p.config.Channel, nsq.NewConfig())
			if err != nil {
				return err
			}

			cs.AddHandler(p.config.HandlerFunc)
		}

		if p.config.EnableProducer {
			pd, err = nsq.NewProducer(p.config.Addr, nsq.NewConfig())
			if err != nil {
				return err
			}
		}

		p.queue = &Queue{
			ctx:      ctx,
			config:   config,
			consumer: cs,
			producer: pd,
		}
		return p.queue.init(p.config.Addr)
	}
	return nil
}

func (p *Provider) Queue() (xqueue.Queue, error) {
	return p.queue, nil
}

func (p *Provider) QueueDestroy() error {
	if p.queue != nil {
		if p.queue.consumer != nil {
			p.queue.consumer.Stop()
		}
		if p.queue.producer != nil {
			p.queue.producer.Stop()
			return nil
		}
	}
	return nil
}

func init() {
	xqueue.Register("nsq", nsqmqpder)
}
