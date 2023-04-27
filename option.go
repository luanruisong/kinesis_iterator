package kinesis_iterator

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

type (
	Option struct {
		Sts        bool   `form:"sts" json:"sts" xml:"sts" mapstructure:"sts"`
		Region     string `form:"region" json:"region" xml:"region" mapstructure:"region"`
		StreamName string `form:"stream_name" json:"stream_name" xml:"stream_name" mapstructure:"stream_name"`
	}
)

func NewOption() *Option {
	return new(Option)
}

func (o *Option) WithSts(b bool) *Option {
	o.Sts = b
	return o
}

func (o *Option) WithRegion(r string) *Option {
	o.Region = r
	return o
}

func (o *Option) WithStreamName(r string) *Option {
	o.StreamName = r
	return o
}

func (o *Option) GetConfig(ctx context.Context) (aws.Config, error) {
	fs := make([]func(*config.LoadOptions) error, 0)
	fs = append(fs, config.WithRegion(o.Region))
	if o.Sts {
		fs = append(fs, config.WithSharedConfigProfile("sts"))
	}
	return config.LoadDefaultConfig(ctx, fs...)
}
