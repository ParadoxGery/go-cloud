package redispubsub

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"sync"
	"time"

	"github.com/go-redis/redis/v7"

	"gocloud.dev/gcerrors"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/driver"
)

func init() {
	o := new(defaultOpener)
	pubsub.DefaultURLMux().RegisterTopic(Scheme, o)
	pubsub.DefaultURLMux().RegisterSubscription(Scheme, o)
}

type defaultOpener struct {
	init   sync.Once
	opener *URLOpener
	err    error
}

func (o *defaultOpener) defaultOpener() (*URLOpener, error) {
	o.init.Do(func() {
		serverURL := os.Getenv("REDIS_SERVER_URL")
		if serverURL == "" {
			o.err = errors.New("REDIS_SERVER_URL environment variable not set")
			return
		}
		client := redis.NewClient(&redis.Options{
			Addr:       serverURL,
			MaxRetries: 5,
		})

		o.opener = &URLOpener{Client: client}
	})

	return o.opener, o.err
}

func (o *defaultOpener) OpenTopicURL(ctx context.Context, u *url.URL) (*pubsub.Topic, error) {
	urlOpener, err := o.defaultOpener()
	if err != nil {
		return nil, fmt.Errorf("open topic '%v': failed to open default connection: %s", u, err)
	}

	return urlOpener.OpenTopicURL(ctx, u)
}

func (o *defaultOpener) OpenSubscriptionURL(ctx context.Context, u *url.URL) (*pubsub.Subscription, error) {
	urlOpener, err := o.defaultOpener()
	if err != nil {
		return nil, fmt.Errorf("open subscription '%v': failed to open default connection: %s", u, err)
	}

	return urlOpener.OpenSubscriptionURL(ctx, u)
}

// Scheme is the URL scheme redispubsub registers its URLOpeners under on pubsub.DefaultMux.
const Scheme = "redis"

// URLOpener opens Redis URLs like "redis://example" for
// topics or subscriptions.
//
// The URL's hos+path is used as the exchange name.
//
// No query parameters are supported.
type URLOpener struct {
	// Client to use for communication with the server.
	Client *redis.Client
}

// OpenTopicURL opens a pubsub.Topic based on u.
func (o *URLOpener) OpenTopicURL(ctx context.Context, u *url.URL) (*pubsub.Topic, error) {
	for p := range u.Query() {
		return nil, fmt.Errorf("open topic '%v': invalid query parameter '%q'", u, p)
	}

	topicName := path.Join(u.Host, u.Path)
	return OpenPub(o.Client, topicName), nil
}

// OpenSubscriptionURL opens pubsub.Subscription based on u.
func (o *URLOpener) OpenSubscriptionURL(ctx context.Context, u *url.URL) (*pubsub.Subscription, error) {
	for p := range u.Query() {
		return nil, fmt.Errorf("open subscription '%v': invalid query parameter '%q'", u, p)
	}

	topicName := path.Join(u.Host, u.Path)
	return OpenSub(o.Client, topicName), nil
}

// OpenPub returns a redis publisher.
func OpenPub(client *redis.Client, name string) *pubsub.Topic {
	return pubsub.NewTopic(&redisPub{c: client, topic: name}, nil)
}

type redisPub struct {
	c     *redis.Client
	topic string
}

func (p *redisPub) SendBatch(ctx context.Context, ms []*driver.Message) error {
	for _, m := range ms {
		c := p.c.Publish(p.topic, m.Body)
		if c.Err() != nil {
			return c.Err()
		}
	}

	return nil
}

func (p *redisPub) IsRetryable(err error) bool {
	// The client handles retries on its own.
	return false
}

func (p *redisPub) As(i interface{}) bool {
	return false // TODO: should be implemented
}

func (p *redisPub) ErrorAs(error, interface{}) bool {
	return false // TODO: maybe can be implemented
}

func (p *redisPub) ErrorCode(error) gcerrors.ErrorCode {
	return gcerrors.Internal // TODO: this should be improved
}

func (p *redisPub) Close() error {
	return nil
}

// OpenSub returns a redis subscriber.
func OpenSub(client *redis.Client, name string) *pubsub.Subscription {
	sub := client.Subscribe(name)
	ch := sub.Channel()
	return pubsub.NewSubscription(&redisSub{sub: sub, ch: ch}, nil, nil)
}

type redisSub struct {
	sub *redis.PubSub
	ch  <-chan *redis.Message
}

func (s *redisSub) ReceiveBatch(ctx context.Context, maxMessages int) ([]*driver.Message, error) {
	var messages []*driver.Message
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
			return messages, nil
		case m := <-s.ch:
			messages = append(messages, &driver.Message{Body: []byte(m.Payload)})
		}

		if len(messages) >= maxMessages {
			return messages, nil
		}
	}
}

func (s *redisSub) SendAcks(ctx context.Context, ackIDs []driver.AckID) error {
	return nil // TODO: research if possible
}

func (s *redisSub) CanNack() bool {
	return false // TODO: research
}

func (s *redisSub) SendNacks(ctx context.Context, ackIDs []driver.AckID) error {
	return nil // TODO: research
}

func (s *redisSub) IsRetryable(err error) bool {
	// The client handles retries on its own.
	return false
}

func (s *redisSub) As(i interface{}) bool {
	return false // TODO: should be implemented
}

func (s *redisSub) ErrorAs(error, interface{}) bool {
	return false // TODO: maybe can be implemented
}

func (s *redisSub) ErrorCode(error) gcerrors.ErrorCode {
	return gcerrors.Internal // TODO: this should be improved
}

func (s *redisSub) Close() error {
	return nil
}
