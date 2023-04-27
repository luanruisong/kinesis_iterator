package kinesis_iterator

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"log"
	"sync"
	"time"
)

type (
	Logger interface {
		Info(string, ...interface{})
		Error(string, ...interface{})
	}

	Iterator struct {
		opt            *Option            //保存原始设置，用来创建client
		done           sync.WaitGroup     //监控当前所有shard的执行状态
		sleepLimit     time.Duration      //getRecord执行间隔
		logger         Logger             // logger
		saver          SequenceSaver      // Sequence执行前后需要对sequenceNumber的操作
		shardInfo      sync.Map           //监控当前执行中shard，以及执行时间
		ctx            context.Context    //总开关ctx
		ctxCancel      context.CancelFunc //总开关ctx cancel
		shardCtxCancel context.CancelFunc //shard 中会监控总开关，以及可以单独关闭shard的ctx
		handler        Handler            //处理函数
	}
	logger  struct{}
	Handler func(data types.Record) error
)

func (l *logger) Info(s string, i ...interface{}) {
	log.Printf(s, i...)
}

func (l *logger) Error(s string, i ...interface{}) {
	log.Printf(s, i...)
}

func NewClient(o *Option) (*kinesis.Client, error) {
	cfg, err := o.GetConfig(context.TODO())
	if err != nil {
		log.Fatalf("Failed to load AWS configuration: %v", err)
		return nil, err
	}
	return kinesis.NewFromConfig(cfg), nil
}

func NewIterator(o *Option) (*Iterator, error) {
	return NewIteratorWithOpt(o), nil
}

func (o *Iterator) monitor() {
	t := time.NewTicker(time.Second * 20)
	cli, err := NewClient(o.opt)
	if err != nil {
		o.logger.Error("[kinesisIteratorMonitor] Failed to get client: %v", err)
		return
	}
	for {
		select {
		case <-o.ctx.Done():
			o.stopAllShard()
			return
		case <-t.C:
			dss, err := cli.DescribeStreamSummary(o.ctx, &kinesis.DescribeStreamSummaryInput{
				StreamName: aws.String(o.StreamName()),
			})
			if err != nil {
				o.logger.Error("[kinesisIteratorMonitor] Failed to describe stream summary: %v", err)
				continue
			}
			count := 0
			o.shardInfo.Range(func(key, value interface{}) bool {
				count++
				o.logger.Info("[kinesisIteratorMonitor] running shard detail,shardId:%s,runningCast:%s", key, time.Now().Sub(time.Unix(value.(int64), 0)).String())
				return true
			})
			switch dss.StreamDescriptionSummary.StreamStatus {
			case types.StreamStatusActive:
				if count == 0 {
					_ = o.doHandle()
				}
			default:
				if count > 0 {
					o.stopAllShard()
				}
			}

			o.logger.Info("[kinesisIteratorMonitor] monitor stream status: %s,running shard count:%d", dss.StreamDescriptionSummary.StreamStatus, count)
		}
	}
}

func (o *Iterator) regShard(shard types.Shard) {
	o.done.Add(1)
	o.shardInfo.Store(*shard.ShardId, time.Now().Unix())
}

func (o *Iterator) defShard(shard types.Shard) {
	o.done.Done()
	o.shardInfo.Delete(*shard.ShardId)
}

func (o *Iterator) StreamName() string {
	return o.opt.StreamName
}

func (o *Iterator) doHandle() error {
	client, err := NewClient(o.opt)
	if err != nil {
		return err
	}
	streamName := o.StreamName()
	// 获取Shard Iterator
	input := &kinesis.ListShardsInput{
		StreamName: aws.String(streamName),
	}
	shardList, err := client.ListShards(o.ctx, input)
	if err != nil {
		o.logger.Error("[kinesisIterator] Failed to get shard iterator: %v", err)
		return err
	}
	var ctx context.Context
	ctx, o.shardCtxCancel = context.WithCancel(context.TODO())
	for _, shard := range shardList.Shards {
		go func(shard types.Shard) {
			o.regShard(shard)
			defer o.defShard(shard)
			ks := NewSequence(*shard.ShardId, streamName, o.saver).Init(o.ctx)
			ks.SetLogger(o.logger)
			o.goShard(ctx, ks, o.handler)
		}(shard)
	}
	return nil
}

func (o *Iterator) Handle(handler Handler) {
	o.handler = handler
}

func (o *Iterator) Run() error {
	err := o.doHandle()
	if err != nil {
		return err
	}
	go o.monitor()
	return nil
}

func (o *Iterator) goShard(ctx context.Context, ks *Sequence, handler Handler) {
	logProxy := fmt.Sprintf("[kinesisIterator-%s]", ks.ShardId)
	o.logger.Info("%s start shard iterator", logProxy)
	client, err := NewClient(o.opt)
	if err != nil {
		o.logger.Error("%s error to get kinesis client %v", logProxy, err)
		return
	}
	input := ks.InitQuery(types.ShardIteratorTypeTrimHorizon)
	iterResp, err := client.GetShardIterator(o.ctx, input)
	if err != nil {
		o.logger.Error("%s failed to get shard iterator: %v", logProxy, err)
		return
	}

	shardIterator := iterResp.ShardIterator
	ticker := time.NewTicker(o.sleepLimit)
	for {
		select {
		case <-o.ctx.Done():
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			output, err := client.GetRecords(o.ctx, &kinesis.GetRecordsInput{
				ShardIterator: shardIterator,
				Limit:         aws.Int32(1000),
			})
			if err != nil {
				if errors.Is(err, context.Canceled) {
					o.logger.Error("%s ctx canceled err:%v", logProxy, err)
					return
				}
				o.logger.Error("%s Failed to get records: %v,sleep:%s", logProxy, err, o.sleepLimit.String())
				iterResp, _ = client.GetShardIterator(o.ctx, ks.InitQuery(types.ShardIteratorTypeTrimHorizon))
				shardIterator = iterResp.ShardIterator
			} else {
				for _, record := range output.Records {
					if err = handler(record); err != nil {
						o.logger.Error("%s Failed to handle records,err: %v", logProxy, err)
					}
				}
				if len(output.Records) > 0 {
					if err := ks.Store(o.ctx, output.Records[len(output.Records)-1].SequenceNumber); err != nil {
						o.logger.Info("%s sync ks error：%v", logProxy, err)
					}
				}
				shardIterator = output.NextShardIterator
				if shardIterator == nil {
					o.logger.Info("%s shard is closed，delete shard sequence", logProxy)
					if err := ks.Stop(o.ctx); err != nil {
						o.logger.Info("%s stop ks error：%v", logProxy, err)
					}
					return
				}
			}
		}
	}
}

func (o *Iterator) stopAllShard() {
	o.shardCtxCancel()
	o.done.Wait()
}

func (o *Iterator) Shutdown(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.TODO(), timeout)
	defer cancel()
	c := make(chan struct{})
	go func() {
		o.ctxCancel()
		o.done.Wait()
		close(c)
	}()
	select {
	case <-ctx.Done():
		return fmt.Errorf("[kinesisIterator] shutdown error:%v", context.Canceled)
	case <-c:
	}
	return nil
}

func (o *Iterator) SetLogger(l Logger) {
	o.logger = l
}

// SetSaver 需要实现saver接口，用来保存shard的seq信息
func (o *Iterator) SetSaver(s SequenceSaver) {
	o.saver = s
}

// SetSleepLimit 设置处理间隔，需要在run之前设置，不然只有重新reShard才可以
func (o *Iterator) SetSleepLimit(duration time.Duration) {
	o.sleepLimit = duration
}

func NewIteratorWithOpt(o *Option) *Iterator {
	c, ctxCancel := context.WithCancel(context.Background())
	return &Iterator{
		opt:        o,
		done:       sync.WaitGroup{},
		sleepLimit: time.Second * 10,
		logger:     &logger{},
		shardInfo:  sync.Map{},
		ctx:        c,
		ctxCancel:  ctxCancel,
	}
}
