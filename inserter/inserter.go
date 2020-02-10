package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	flow "github.com/demonix/goflow/pb"
	proto "github.com/golang/protobuf/proto"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/zmap/go-iptree/iptree"
)

var (
	LogLevel = flag.String("loglevel", "info", "Log level")

	MetricsAddr = flag.String("metrics.addr", ":8081", "Metrics address")
	MetricsPath = flag.String("metrics.path", "/metrics", "Metrics path")

	KafkaTopic = flag.String("kafka.topic", "flows-processed", "Kafka topic to consume from")
	KafkaBrk   = flag.String("kafka.brokers", "127.0.0.1:9092,[::1]:9092", "Kafka brokers list separated by commas")
	KafkaGroup = flag.String("kafka.group", "postgres-inserter", "Kafka group id")
	FlushTime  = flag.String("flush.dur", "5s", "Flush duration")
	FlushCount = flag.Int("flush.count", 100, "Flush count")

	PostgresUser    = flag.String("postgres.user", "postgres", "Postgres user")
	PostgresPass    = flag.String("postgres.pass", "", "Postgres password")
	PostgresHost    = flag.String("postgres.host", "127.0.0.1", "Postgres host")
	PostgresPort    = flag.Int("postgres.port", 5432, "Postgres port")
	PostgresDbName  = flag.String("postgres.dbname", "postgres", "Postgres database")
	PrivateIPBlocks []*net.IPNet
	ProjectIPBlocks map[string]string = make(map[string]string)
	ProjectIPS      map[string]string = make(map[string]string)

	NetTree *iptree.IPTree

	Inserts = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "insert_count",
			Help: "Inserts made to Postgres.",
		},
	)

	flow_fields = []string{
		"date_inserted",
		"time_flow",
		"type",
		"sampling_rate",
		"src_ip",
		"dst_ip",
		"src_prj",
		"dst_prj",
		"bytes",
		"packets",
		"src_port",
		"dst_port",
		"etype",
		"proto",
		"src_as",
		"dst_as",
		"sampler_address",
	}
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

	flows    [][]interface{}
	offstash *cluster.OffsetStash
	consumer *cluster.Consumer
}

func (s *state) flush() bool {
	log.Infof("Processed %d records in the last iteration with bulk.", s.msgCount)
	s.msgCount = 0

	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyIn("flows", flow_fields...))
	if err != nil {
		log.Fatal(err)
	}

	for _, curFlow := range s.flows {
		_, err = stmt.Exec(curFlow...)
		if err != nil {
			log.Debugf("TEST %v", curFlow)
			log.Fatal(err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}

	s.consumer.MarkOffsets(s.offstash)
	s.offstash = cluster.NewOffsetStash()
	s.flows = make([][]interface{}, 0)
	return true
}

func (s *state) flushOld() bool {
	log.Infof("Processed %d records in the last iteration.", s.msgCount)
	s.msgCount = 0

	flows_replace := make([]string, len(flow_fields))
	for i := range flow_fields {
		flows_replace[i] = fmt.Sprintf("$%v", i+1)
	}
	query := fmt.Sprintf("INSERT INTO flows (%v) VALUES (%v)", strings.Join(flow_fields, ", "), strings.Join(flows_replace, ", "))
	for _, curFlow := range s.flows {
		_, err := s.db.Exec(query, curFlow...)
		if err != nil {
			log.Debugf("TEST %v", curFlow)
			log.Fatal(err)
		}
	}

	s.consumer.MarkOffsets(s.offstash)
	s.offstash = cluster.NewOffsetStash()
	s.flows = make([][]interface{}, 0)
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
		ts := time.Unix(int64(fmsg.TimeFlowEnd), 0)
		timeNow := time.Now

		srcip := net.IP(fmsg.SrcAddr)
		dstip := net.IP(fmsg.DstAddr)
		sampleraddress := net.IP(fmsg.SamplerAddress)

		srcipstr := srcip.String()
		dstipstr := dstip.String()
		sampleraddressstr := sampleraddress.String()

		if srcipstr == "<nil>" {
			srcipstr = "0.0.0.0"
		}
		if dstipstr == "<nil>" {
			dstipstr = "0.0.0.0"
		}
		if sampleraddressstr == "<nil>" {
			sampleraddressstr = "0.0.0.0"
		}
		srcprj := getPrj(srcipstr)
		dstprj := getPrj(dstipstr)

		extract := []interface{}{
			timeNow,
			ts,
			fmsg.Type,
			fmsg.SamplingRate,
			srcipstr,
			dstipstr,
			srcprj,
			dstprj,
			fmsg.Bytes,
			fmsg.Packets,
			fmsg.SrcPort,
			fmsg.DstPort,
			fmsg.Etype,
			fmsg.Proto,
			fmsg.SrcAS,
			fmsg.DstAS,
			sampleraddressstr,
		}
		s.flows = append(s.flows, extract)
	}
	s.offstash.MarkOffset(msg, "")

	return false, nil, cur
}

func closeAll(db *sql.DB, consumer *cluster.Consumer) {
	consumer.Close()
	db.Close()
}

func getPrj(ip string) (project string) {
	proj, found, _ := NetTree.GetByString(ip)

	if found {
		return proj.(string)
	}
	return "UnknownNetwork"
}

func initNetworks() {

	NetTree = iptree.New()

	NetTree.AddByString("0.0.0.0/0", "ExternalNetwork")
	NetTree.AddByString("127.0.0.0/8", "Loopback")
	NetTree.AddByString("10.0.0.0/8", "PrivateNet")
	NetTree.AddByString("172.16.0.0/12", "PrivateNet")
	NetTree.AddByString("192.168.0.0/16", "PrivateNet")
	NetTree.AddByString("169.254.0.0/16", "LinkLocal")
	NetTree.AddByString("::1/128", "Loopback")
	NetTree.AddByString("fe80::/10", "LinkLocal")
	NetTree.AddByString("fc00::/7", "IPv6Local")

	NetTree.AddByString("fc00::/7", "IPv6Local")

	fileHandle, _ := os.Open("/etc/nets.txt")
	defer fileHandle.Close()
	fileScanner := bufio.NewScanner(fileHandle)
	cnt := 0

	for fileScanner.Scan() {
		line := strings.Split(fileScanner.Text(), "|")
		NetTree.AddByString(line[0], line[1])
		log.Info("Loaded net %s: %s", line[0], line[1])
		cnt++
	}
	log.Info("Loaded %s records", cnt)

}

func main() {
	initNetworks()
	flag.Parse()

	lvl, _ := log.ParseLevel(*LogLevel)
	log.SetLevel(lvl)

	s := &state{
		last:     time.Time{},
		offstash: cluster.NewOffsetStash(),
	}
	go s.metricsHTTP()

	config := cluster.NewConfig()
	brokers := strings.Split(*KafkaBrk, ",")
	topics := []string{*KafkaTopic}
	consumer, err := cluster.NewConsumer(brokers, *KafkaGroup, topics, config)
	if err != nil {
		log.Fatal(err)
	}
	s.consumer = consumer

	pg_pass := *PostgresPass
	if pg_pass == "" {
		log.Debugf("Postgres password argument unset, using environment variable $POSTGRES_PASSWORD")
		pg_pass = os.Getenv("POSTGRES_PASSWORD")
	}

	info := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		*PostgresHost, *PostgresPort, *PostgresUser, pg_pass, *PostgresDbName)
	db, err := sql.Open("postgres", info)
	if err != nil {
		log.Fatal(err)
	}
	s.db = db
	defer closeAll(db, consumer)

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
