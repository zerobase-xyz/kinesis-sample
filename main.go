package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type Person struct {
	Name string `json:"name"`
}

func main() {
	var (
		person     Person
		streamName       = "Hoge"
		shardType        = "LATEST"
		limit      int64 = 1
	)

	sess := session.Must(session.NewSession())
	svc := kinesis.New(sess, aws.NewConfig().WithRegion("ap-northeast-1"))

	stream, _ := svc.DescribeStream(&kinesis.DescribeStreamInput{StreamName: &streamName})
	shard := stream.StreamDescription.Shards[0].ShardId
	lastIter, _ := svc.GetShardIterator(&kinesis.GetShardIteratorInput{ShardId: shard, ShardIteratorType: &shardType, StreamName: &streamName})
	Iter := lastIter.ShardIterator
	for {
		if Iter != nil {
			res, _ := svc.GetRecords(&kinesis.GetRecordsInput{ShardIterator: Iter, Limit: &limit})
			if len(res.Records) != 0 {
				data := res.Records[0].Data
				json.Unmarshal(data, &person)
				fmt.Printf("\n%s\n", person.Name)
			}
			Iter = res.NextShardIterator

		}
		fmt.Print("...")
		time.Sleep(1 * time.Second)
	}
}
