package main

import (
	"encoding/json"
	"io/ioutil"
	"time"

	"github.com/gogap/logs"
	"github.com/qjpcpu/ali_mns"
)

type appConf struct {
	Url             string `json:"url"`
	AccessKeyId     string `json:"access_key_id"`
	AccessKeySecret string `json:"access_key_secret"`
}

func main() {
	conf := appConf{}

	if bFile, e := ioutil.ReadFile("app.conf"); e != nil {
		panic(e)
	} else {
		if e := json.Unmarshal(bFile, &conf); e != nil {
			panic(e)
		}
	}

	client := ali_mns.NewAliMNSClient(conf.Url,
		conf.AccessKeyId,
		conf.AccessKeySecret)

	queue := ali_mns.NewMNSQueue("queue-test-1", client)

	msg := ali_mns.TopicMessageSendRequest{
		MessageBody: []byte("BIG TEST"),
	}
	topic := ali_mns.NewMNSTopic("topic-test1", client)
	ret, err := topic.SendMessage(msg)

	if err != nil {
		logs.Error(err)
	} else {
		logs.Pretty("response:", ret)
	}

	respChan := make(chan ali_mns.MessageReceiveResponse)
	errChan := make(chan error)
	go func() {
		for {
			select {
			case resp := <-respChan:
				{
					logs.Pretty("response:", resp)
					logs.Pretty("message body:", string(resp.MessageBody))
					logs.Debug("change the visibility: ", resp.ReceiptHandle)
					if ret, e := queue.ChangeMessageVisibility(resp.ReceiptHandle, 30); e != nil {
						logs.Error(e)
					} else {
						logs.Pretty("visibility changed", ret)
					}

					logs.Debug("delete it now: ", resp.ReceiptHandle)
					if e := queue.DeleteMessage(resp.ReceiptHandle); e != nil {
						logs.Error(e)
					}
				}
			case err := <-errChan:
				{
					logs.Error(err)
				}
			}
		}

	}()

	queue.ReceiveMessage(respChan, errChan)
	for {
		time.Sleep(time.Second * 1)
	}

}
