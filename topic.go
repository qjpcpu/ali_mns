package ali_mns

import (
	"fmt"
	"os"
	"strings"
	"time"
)

type AliMNSTopic struct {
	name       string
	client     MNSClient
	qpsLimit   int32
	qpsMonitor *QPSMonitor
	decoder    MNSDecoder
}

func NewMNSTopic(name string, client MNSClient, qps ...int32) *AliMNSTopic {
	if name == "" {
		panic("ali_mns: topic name could not be empty")
	}

	topic := new(AliMNSTopic)
	topic.client = client
	topic.name = name
	topic.qpsLimit = DefaultQPSLimit
	topic.decoder = NewAliMNSDecoder()

	if qps != nil && len(qps) == 1 && qps[0] > 0 {
		topic.qpsLimit = qps[0]
	}

	proxyURL := ""
	queueProxyEnvKey := PROXY_PREFIX + strings.Replace(strings.ToUpper(name), "-", "_", -1)
	if url := os.Getenv(queueProxyEnvKey); url != "" {
		proxyURL = url
	} else if globalurl := os.Getenv(GLOBAL_PROXY); globalurl != "" {
		proxyURL = globalurl
	}

	if proxyURL != "" {
		topic.client.SetProxy(proxyURL)
	}

	topic.qpsMonitor = NewQPSMonitor(5)

	return topic
}

func (p *AliMNSTopic) Name() string {
	return p.name
}

func (p *AliMNSTopic) SendMessage(message TopicMessageSendRequest) (resp MessageSendResponse, err error) {
	p.checkQPS()
	_, err = send(p.client, p.decoder, POST, nil, message, fmt.Sprintf("topics/%s/%s", p.name, "messages"), &resp)
	return
}

func (p *AliMNSTopic) checkQPS() {
	p.qpsMonitor.Pulse()
	if p.qpsLimit > 0 {
		for p.qpsMonitor.QPS() > p.qpsLimit {
			time.Sleep(time.Millisecond * 10)
		}
	}
}
