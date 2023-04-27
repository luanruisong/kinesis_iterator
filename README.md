# kinesis_iterator

## aws kinesis 保姆级封装

 - 默认TRIM_HORIZON，根据Saver处理切换为AFTER_SEQUENCE
 - reShard 关闭退出当前读取的Shard，并等待active 重新拉起
 - SequenceSaver 支持自定义写入/读取/删除 sequece
 - 更注重核心逻辑

## usage
```go
//设置option
opt := NewOption().WithRegion("us-west-2").WithSts(true).WithStreamName(streamName)

//创建iterator
iter, err := NewIterator(opt)

if err != nil {
    panic(err)
}

//设置读取间隔
iter.SetSleepLimit(time.Second)

//设置处理函数
iter.Handle(func(data types.Record) error {
	//TODO
    return nil
})

//启动任务
if err := iter.Run(); err != nil {
    panic(err)
}

//shutdown
time.Sleep(time.Second * 10)
iter.Shutdown(time.Second * 2)
```

