package kinesis_iterator

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type Sequence struct {
	sequence   *string       //sequencenumber ，aws的position
	StreamName string        //streamName
	ShardId    string        //当前Shard的id
	saver      SequenceSaver //saver
	reTry      int           //重试次数
	logger     Logger        //logger
}

type SequenceSaver interface {
	Get(ctx context.Context, streamName, shardId string) string
	Del(ctx context.Context, streamName, shardId string) error
	Set(ctx context.Context, streamName, shardId, sequence string) error
}

func (ks *Sequence) Init(ctx context.Context) *Sequence {
	if ks.saver == nil {
		return ks
	}
	value := ks.saver.Get(ctx, ks.StreamName, ks.ShardId)
	if len(value) > 0 {
		ks.sequence = &value
		ks.logger.Info("[kinesisIterator] init sequence success,sequenceNumber:%s", value)
	}
	return ks
}

func (ks *Sequence) Get() *string {
	return ks.sequence
}

func (ks *Sequence) ReTry(reTry int) {
	ks.reTry = reTry
}

func (ks *Sequence) Store(ctx context.Context, sequence *string) error {
	ks.sequence = sequence
	return ks.sync(ctx)
}

func (ks *Sequence) Stop(ctx context.Context) error {
	return ks.saver.Del(ctx, ks.StreamName, ks.ShardId)
}

func (ks *Sequence) sync(ctx context.Context) error {
	if ks.sequence == nil {
		return errors.New("sequence number is nil")
	}
	if ks.saver == nil {
		return errors.New("sequence saver is nil")
	}
	var err error
	for i := 0; i < ks.reTry; i++ {
		err := ks.saver.Set(ctx, ks.StreamName, ks.ShardId, *ks.sequence)
		if err == nil {
			ks.logger.Info("[kinesisIterator] sync sequenceNumber:%s success", *ks.sequence)
			return nil
		}
	}
	ks.logger.Error("[kinesisIterator] sync sequence error,err:%v,key:%s,sequenceNumber:%s", err, *ks.sequence)
	return err
}

func (ks *Sequence) InitQuery(defType types.ShardIteratorType) *kinesis.GetShardIteratorInput {
	query := &kinesis.GetShardIteratorInput{
		ShardId:                aws.String(ks.ShardId),
		ShardIteratorType:      defType,
		StartingSequenceNumber: nil,
		StreamARN:              nil,
		StreamName:             aws.String(ks.StreamName),
		Timestamp:              nil,
	}
	if sn := ks.Get(); sn != nil {
		query.ShardIteratorType = types.ShardIteratorTypeAfterSequenceNumber
		query.StartingSequenceNumber = sn
	}
	ks.logger.Info("[kinesisIterator] init default query,shardId:%s,streamName:%s,shardIteratorType:%s", *query.ShardId, *query.StreamName, query.ShardIteratorType)
	return query
}

func (ks *Sequence) SetLogger(l Logger) {
	ks.logger = l
}

func NewSequence(shardId, streamName string, saver SequenceSaver) *Sequence {
	return &Sequence{
		saver:      saver,
		reTry:      3,
		ShardId:    shardId,
		StreamName: streamName,
		logger:     &logger{},
	}
}
