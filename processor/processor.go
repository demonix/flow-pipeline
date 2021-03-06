package main

import (
	"database/sql"
	"flag"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	sarama "github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	flow "github.com/demonix/goflow/pb"
	proto "github.com/golang/protobuf/proto"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

var (
	LogLevel = flag.String("loglevel", "info", "Log level")

	MetricsAddr = flag.String("metrics.addr", ":8081", "Metrics address")
	MetricsPath = flag.String("metrics.path", "/metrics", "Metrics path")

	InKafkaTopic = flag.String("kafka.input.topic", "flows", "Kafka topic to consume from")
	InKafkaBrk   = flag.String("kafka.input.brokers", "127.0.0.1:9092,[::1]:9092", "Kafka brokers list separated by commas")
	InKafkaGroup = flag.String("kafka.input.group", "flows-processor", "Kafka group id")

	OutKafkaTopic = flag.String("kafka.output.topic", "flows-processed", "Kafka topic to consume from")
	OutKafkaBrk   = flag.String("kafka.output.brokers", "127.0.0.1:9092,[::1]:9092", "Kafka brokers list separated by commas")

	FlushTime  = flag.String("flush.dur", "5s", "Flush duration")
	FlushCount = flag.Int("flush.count", 100, "Flush count")

	PostgresUser   = flag.String("postgres.user", "postgres", "Postgres user")
	PostgresPass   = flag.String("postgres.pass", "", "Postgres password")
	PostgresHost   = flag.String("postgres.host", "127.0.0.1", "Postgres host")
	PostgresPort   = flag.Int("postgres.port", 5432, "Postgres port")
	PostgresDbName = flag.String("postgres.dbname", "postgres", "Postgres database")

	Inserts = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "insert_count",
			Help: "Inserts made to Postgres.",
		},
	)
)

func (s *state) metricsHTTP() {
	prometheus.MustRegister(Inserts)
	http.Handle(*MetricsPath, promhttp.Handler())
	log.Fatal(http.ListenAndServe(*MetricsAddr, nil))
}

type state struct {
	msgCount int
	last     time.Time
	dur      time.Duration

	db *sql.DB

	flows         []flow.FlowMessage
	offstash      *cluster.OffsetStash
	consumer      *cluster.Consumer
	producer      sarama.AsyncProducer
	networks      map[string]string
	OutKafkaTopic *string
}

func (s *state) flush() bool {
	log.Infof("Processed %d records in the last iteration.", s.msgCount)
	s.msgCount = 0

	for _, curFlow := range s.flows {

		b, _ := proto.Marshal(&curFlow)

		s.producer.Input() <- &sarama.ProducerMessage{
			Topic: *s.OutKafkaTopic,
			Value: sarama.ByteEncoder(b),
		}
	}

	s.consumer.MarkOffsets(s.offstash)
	s.offstash = cluster.NewOffsetStash()
	s.flows = make([]flow.FlowMessage, 0)
	return true
}

func (s *state) buffer(msg *sarama.ConsumerMessage, cur time.Time) (bool, error, time.Time) {
	s.msgCount++

	var fmsg flow.FlowMessage

	err := proto.Unmarshal(msg.Value, &fmsg)
	if err != nil {
		log.Printf("unmarshaling error: ", err)
	} else {
		log.Debug(fmsg)

		srcip := net.IP(fmsg.SrcAddr)
		dstip := net.IP(fmsg.DstAddr)
		if fmsg.SrcPort == 0 {
			if fmsg.UdpSrcPort != 0 {
				fmsg.SrcPort = fmsg.UdpSrcPort
			}
			if fmsg.TcpSrcPort != 0 {
				fmsg.SrcPort = fmsg.TcpSrcPort
			}
		}
		if fmsg.DstPort == 0 {
			if fmsg.UdpDstPort != 0 {
				fmsg.DstPort = fmsg.UdpDstPort
			}
			if fmsg.TcpDstPort != 0 {
				fmsg.DstPort = fmsg.TcpDstPort
			}
		}

		srcipstr := srcip.String()
		dstipstr := dstip.String()

		if srcipstr == "<nil>" {
			srcipstr = "0.0.0.0"
		}
		if dstipstr == "<nil>" {
			dstipstr = "0.0.0.0"
		}
		fmsg.SrcIpStr = srcipstr
		fmsg.DstIpStr = dstipstr

		s.flows = append(s.flows, fmsg)
	}
	s.offstash.MarkOffset(msg, "")

	return false, nil, cur
}

func closeAll(consumer *cluster.Consumer, producer sarama.AsyncProducer) {
	consumer.Close()
	producer.Close()
}

func main() {
	flag.Parse()

	lvl, _ := log.ParseLevel(*LogLevel)
	log.SetLevel(lvl)

	s := &state{
		last:     time.Time{},
		offstash: cluster.NewOffsetStash(),
	}
	s.networks = getNetWorks()
	s.OutKafkaTopic = OutKafkaTopic
	go s.metricsHTTP()

	config := cluster.NewConfig()
	inBrokers := strings.Split(*InKafkaBrk, ",")
	inTopics := []string{*InKafkaTopic}
	consumer, err := cluster.NewConsumer(inBrokers, *InKafkaGroup, inTopics, config)
	if err != nil {
		log.Fatal(err)
	}
	s.consumer = consumer

	outBrokers := strings.Split(*OutKafkaBrk, ",")
	sarmaconfig := sarama.NewConfig()
	sarmaconfig.Producer.Return.Errors = true
	sarmaconfig.Producer.Return.Successes = false
	producer, err := sarama.NewAsyncProducer(outBrokers, sarmaconfig)
	log.Infof("Trying to connect to Kafka: %v", outBrokers)
	if err != nil {
		log.Fatal(err)
	}
	s.producer = producer

	go func() {
		for {
			select {
			case err := <-producer.Errors():
				log.Error(err)
			}
		}
	}()

	defer closeAll(consumer, producer)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	durFlush, _ := time.ParseDuration(*FlushTime)
	var count int
	timer := time.After(durFlush)

	for {
		select {
		case <-timer:
			s.flush()
			timer = time.After(durFlush)
		case msg, ok := <-consumer.Messages():
			if ok {
				log.Debugf("%s/%d/%d\t%s\t", msg.Topic, msg.Partition, msg.Offset, msg.Key)
				flush, err, _ := s.buffer(msg, time.Now().UTC())
				if flush {
					s.flush()
				}
				if err != nil {
					log.Errorf("Error while processing: %v", err)
				}
				count++
				if count == *FlushCount {
					s.flush()
					count = 0
				}
			}
		case <-signals:
			return
		}
	}
	log.Info("Stopped processing")
}
