# kinesis_iterator

## aws kinesis 保姆级封装迭代器

 - 默认TRIM_HORIZON，根据Saver处理切换为AFTER_SEQUENCE
 - reShard 关闭退出当前读取的Shard，并等待active 重新拉起
 - SequenceSaver 支持自定义写入/读取/删除 sequece
 - 更注重核心逻辑

## 使用

### 安装

```shell
go get -u github.com/luanruisong/kinesis_iterator
```

### 代码示例

引入

```go
import (
    "github.com/luanruisong/kinesis_iterator"
)
```

demo

```go


//设置option
opt := kinesis_iterator.NewOption().WithRegion("us-west-2").WithSts(true).WithStreamName(streamName)

//创建iterator
iter, err := kinesis_iterator.NewIterator(opt)

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

## 贡献
如果您想为该项目做出贡献，请参考以下步骤：

- Fork该项目
- 创建一个新的分支
- 对代码进行更改并确保所有测试都通过
- 提交一个pull request 

我们欢迎所有形式的贡献，包括错误修复、功能增强和文档改进。感谢您的贡献！
