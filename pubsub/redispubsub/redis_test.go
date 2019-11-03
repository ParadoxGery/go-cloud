package redispubsub_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	_ "gocloud.dev/pubsub/redispubsub"

	"github.com/alicebob/miniredis"
	"gocloud.dev/pubsub"
)

func Test_Publish(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatal("could not start miniredis")
	}
	defer s.Close()

	os.Setenv("REDIS_SERVER_URL", s.Addr())

	topic, err := pubsub.OpenTopic(context.TODO(), "redis://test")
	if err != nil {
		t.Fatal(err)
	}

	err = topic.Send(context.TODO(), &pubsub.Message{
		Body: []byte("hallo"),
	})

	if err != nil {
		t.Error(err)
	}
}

func Test_Subscribe(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatal("could not start miniredis")
	}
	defer s.Close()

	os.Setenv("REDIS_SERVER_URL", s.Addr())

	subs, err := pubsub.OpenSubscription(context.TODO(), "redis://test")
	if err != nil {
		t.Fatal(err)
	}

	topic, err := pubsub.OpenTopic(context.TODO(), "redis://test")
	if err != nil {
		t.Fatal(err)
	}

	err = topic.Send(context.TODO(), &pubsub.Message{
		Body: []byte("hallo"),
	})
	if err != nil {
		t.Error(err)
	}

	m, err := subs.Receive(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(m)
}
