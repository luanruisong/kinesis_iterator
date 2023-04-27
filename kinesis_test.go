package kinesis_iterator

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"testing"
	"time"
)

const streamName = "your-stream-name"

func TestNewIteratorWithClient(t *testing.T) {

	opt := NewOption().WithRegion("us-west-2").WithSts(true).WithStreamName(streamName)

	ctx, _ := context.WithCancel(context.TODO())

	iter, err := NewIterator(ctx, opt)
	iter.SetSleepLimit(time.Second)
	if err != nil {
		panic(err)
	}
	iter.Handle(func(data types.Record) error {
		//fmt.Println(string(data.Data))
		return nil
	})

	go func() {
		time.Sleep(time.Second * 10)
		fmt.Println("123123123123123")
		iter.Shutdown(time.Second * 2)
	}()

	if err := iter.Run(); err != nil {
		panic(err)
	}
	select {}
}
