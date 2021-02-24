package nsq

import (
	"context"
	"github.com/navi-tt/xqueue"
	"golang.org/x/sync/errgroup"
	"testing"
	"time"
)

var config = xqueue.QueueConfig{
	Topic:      "NSQ_TOPIC",
	Tags:       []string{},
	InstanceId: "",
	GroupId:    "",
	ProviderJsonConfig: `{
    "enable_consumer": true,
    "enable_producer": true,
    "addr": "172.24.145.37:4150",
    "channel": "NSQ_CHANNEL"
}`,
}

func initNsqmq() (*Provider, error) {
	p, err := xqueue.GetProvider("nsq")
	if err != nil {
		return nil, err
	}

	return p.(*Provider), nil
}

func TestQueue_Enqueue(t *testing.T) {
	var (
		eg, ctx = errgroup.WithContext(context.Background())
	)
	p, err := initNsqmq()
	if err != nil {
		t.Fatal(err)
	}

	err = p.QueueInit(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	q, err := p.Queue()
	if err != nil {
		t.Fatal(err)
	}

	eg.Go(func() error {
		return q.Enqueue(ctx, &Message{
			topic:     config.Topic,
			groupId:   "",
			tags:      []string{},
			data:      []byte("nice!~~"),
			messageId: "",
		})
	})

	eg.Go(func() error {
		time.Sleep(1 * time.Second)
		msg, err := q.Dequeue(ctx)
		if err != nil {
			return err
		}
		t.Logf("data: %+v | id : %v", string(msg.GetData()),msg.MessageId())
		return nil
	})
	err = eg.Wait()
	if err != nil {
		t.Fatal(err)
	}
}
