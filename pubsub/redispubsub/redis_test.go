package redispubsub_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	_ "gocloud.dev/pubsub/redispubsub"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v7"
	"gocloud.dev/pubsub"
)

var initSync sync.Once
var server *miniredis.Miniredis

func createMiniredis() (*miniredis.Miniredis, error) {
	var err error
	initSync.Do(func() {
		server, err = miniredis.Run()
	})

	return server, err
}

func Test_Publish(t *testing.T) {
	s, err := createMiniredis()
	if err != nil {
		t.Fatal("could not start miniredis")
	}

	os.Setenv("REDIS_SERVER_URL", s.Addr())

	topic, err := pubsub.OpenTopic(context.TODO(), "redis://test")
	if err != nil {
		t.Fatal(err)
	}

	err = topic.Send(context.TODO(), &pubsub.Message{
		Body: []byte("hello world!"),
	})

	if err != nil {
		t.Error(err)
	}
}

func Test_Subscribe(t *testing.T) {
	s, err := createMiniredis()
	if err != nil {
		t.Fatal("could not start miniredis")
	}

	os.Setenv("REDIS_SERVER_URL", s.Addr())

	subs, err := pubsub.OpenSubscription(context.TODO(), "redis://test")
	if err != nil {
		t.Fatal(err)
	}
	defer subs.Shutdown(context.TODO())

	topic, err := pubsub.OpenTopic(context.TODO(), "redis://test")
	if err != nil {
		t.Fatal(err)
	}
	defer topic.Shutdown(context.TODO())

	var body []byte = []byte("hello world!")

	err = topic.Send(context.TODO(), &pubsub.Message{
		Body: body,
	})
	if err != nil {
		t.Error(err)
	}

	m, err := subs.Receive(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	if string(m.Body) != string(body) {
		t.Errorf("expected body '%s' but got '%s'", body, m.Body)
	}
}

func Test_Publish_InvalidURL(t *testing.T) {
	s, err := createMiniredis()
	if err != nil {
		t.Fatal("could not start miniredis")
	}

	os.Setenv("REDIS_SERVER_URL", s.Addr())

	_, err = pubsub.OpenTopic(context.Background(), "redis://test.topic?thing=invalid")

	expectedErr := fmt.Errorf("invalid query parameter \"thing\"")

	if !strings.HasSuffix(err.Error(), expectedErr.Error()) {
		t.Fatalf("expected '%s' as part of the error, but got '%s'", expectedErr, err)
	}
}

func Test_Subscribe_InvalidURL(t *testing.T) {
	s, err := createMiniredis()
	if err != nil {
		t.Fatal("could not start miniredis")
	}

	os.Setenv("REDIS_SERVER_URL", s.Addr())

	_, err = pubsub.OpenSubscription(context.Background(), "redis://test.topic?thing=invalid")

	expectedErr := fmt.Errorf("invalid query parameter \"thing\"")

	if !strings.HasSuffix(err.Error(), expectedErr.Error()) {
		t.Fatalf("expected '%s' as part of the error, but got '%s'", expectedErr, err)
	}
}

func Test_Subscribe_As(t *testing.T) {
	s, err := createMiniredis()
	if err != nil {
		t.Fatal(err)
	}

	os.Setenv("REDIS_SERVER_URL", s.Addr())

	sub, err := pubsub.OpenSubscription(context.TODO(), "redis://testing")
	if err != nil {
		t.Fatal(err)
	}

	var psub *redis.PubSub
	sub.As(&psub)

	err = psub.Ping()
	if err != nil {
		t.Fatal(err)
	}
}

func Test_Publish_As(t *testing.T) {
	s, err := createMiniredis()
	if err != nil {
		t.Fatal(err)
	}

	os.Setenv("REDIS_SERVER_URL", s.Addr())

	pub, err := pubsub.OpenTopic(context.TODO(), "redis://pub-as")
	if err != nil {
		t.Fatal(err)
	}

	var c *redis.Client
	pub.As(&c)

	cmd := c.Ping()
	if cmd.Err() != nil {
		t.Fatal(cmd.Err())
	}
}
