package main

import (
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

		srcip := net.IP(fmsg.SrcAddr)
		dstip := net.IP(fmsg.DstAddr)
		srcipstr := srcip.String()
		dstipstr := dstip.String()
		if srcipstr == "<nil>" {
			srcipstr = "0.0.0.0"
		}
		if dstipstr == "<nil>" {
			dstipstr = "0.0.0.0"
		}
		srcprj := getPrj(srcipstr)
		dstprj := getPrj(dstipstr)
		extract := []interface{}{
			"NOW()",
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
	prj, found := ProjectIPS[ip]
	if found {
		return prj
	}
	for _, network := range PrivateIPBlocks {
		if network.Contains(net.ParseIP(ip)) {
			return "InternalNetwork"
		}
	}
	return "PublicNetwork"
}

func parseProjectNet(netCidr string, projectName string) (networkBlock *net.IPNet, projName string) {
	_, block, err := net.ParseCIDR(netCidr)
	if err != nil {
		panic(fmt.Errorf("parse error on %q: %v", netCidr, err))
	}
	return block, projectName
}

func initNetworks() {

	for _, cidr := range []string{
		"127.0.0.0/8",    // IPv4 loopback
		"10.0.0.0/8",     // RFC1918
		"172.16.0.0/12",  // RFC1918
		"192.168.0.0/16", // RFC1918
		"169.254.0.0/16", // RFC3927 link-local
		"::1/128",        // IPv6 loopback
		"fe80::/10",      // IPv6 link-local
		"fc00::/7",       // IPv6 unique local addr
	} {
		_, block, err := net.ParseCIDR(cidr)
		if err != nil {
			panic(fmt.Errorf("parse error on %q: %v", cidr, err))
		}
		PrivateIPBlocks = append(PrivateIPBlocks, block)
	}

	ProjectIPBlocks = map[string]string{
		"185.161.180.0/22": "OurExternalNetwork",
		"46.17.200.0/21":   "OurExternalNetwork",
		"10.217.0.0/16":    "DevTest",
		"10.17.0.0/24":     "KEInfra",
		"10.17.2.0/24":     "WIC",
		"10.17.3.0/24":     "Vostok",
		"10.17.4.0/24":     "Mercury",
		"10.17.5.0/24":     "Expert",
		"10.17.6.0/24":     "BG",
		"10.17.8.0/24":     "Analytics",
		"10.17.9.0/24":     "Pravo",
		"10.17.10.0/24":    "FOCUS",
		"10.18.3.0/24":     "Elba",
		"10.18.4.0/24":     "KE-Api",
		"10.18.5.0/24":     "KEWeb",
		"10.18.6.0/24":     "SPKO",
		"10.18.7.0/24":     "Realty",
		"10.18.8.0/24":     "KS",
		"10.18.9.0/24":     "SRS",
		"10.18.10.0/24":    "Coza",
		"10.18.11.0/24":    "Ulearn",
		"10.18.12.0/24":    "MEDI",
		"10.18.14.0/24":    "DCApi",
		"10.18.17.0/24":    "Metrika",
		"10.18.18.0/24":    "UKS",
		"10.18.19.0/24":    "Zakupki",
		"10.18.20.0/24":    "Info",
		"10.18.21.0/24":    "Click",
		"10.18.22.0/23":    "Market",
		"10.18.24.0/24":    "Queue",
		"10.18.27.0/24":    "KK",
		"10.18.28.0/24":    "International",
		"10.18.29.0/24":    "Evrika",
		"10.18.30.0/24":    "Normativ",
		"10.18.31.0/24":    "Websites",
		"10.18.32.0/24":    "QuickCRM",
		"10.18.33.0/24":    "Travel",
		"10.18.34.0/24":    "K84",
		"10.20.1.0/24":     "Diadoc",
		"10.81.8.0/24":     "XLT_Testing",
		"192.168.120.0/24": "Test",
		"192.168.127.0/24": "Admin-Svcs",
		"192.168.128.0/23": "Hypervisors",
		"192.168.132.0/24": "VM-Prints",
		"192.168.133.0/24": "etc",
		"192.168.136.0/21": "IPMIs-Only",
		"192.168.148.0/22": "Production-Phy",
		"192.168.156.0/22": "Diffs",
		"192.168.162.0/24": "Safe-Test",
		"192.168.163.0/24": "Postavki",
		"192.168.168.0/24": "Kanso3D",
		"192.168.169.0/24": "VM-Forms",
		"192.168.170.0/24": "VM-Billing",
		"192.168.171.0/24": "OIR",
		"192.168.173.0/24": "Devops",
		"192.168.188.0/22": "EDI",
		"10.64.1.0/24":     "Hypervisors",
		"10.64.6.0/24":     "MSUP",
		"10.64.17.0/24":    "OIR",
		"10.64.18.0/24":    "KEInfra",
		"10.64.19.0/24":    "KEAPI",
		"10.64.20.0/24":    "KEWeb",
		"10.64.21.0/24":    "DevOps",
		"10.64.22.0/24":    "Kanso3D",
		"10.64.23.0/24":    "Realty",
		"10.64.24.0/24":    "Evrika",
		"10.64.26.0/24":    "KonturOFD",
		"10.64.29.0/24":    "KOPF-VM",
		"10.64.30.0/24":    "KonturNTTOFD-VM",
		"10.64.31.0/24":    "KonturNTTOFD-PM",
		"10.64.33.0/24":    "KS",
		"10.64.34.0/24":    "Coza",
		"10.64.35.0/24":    "Focus",
		"10.64.36.0/24":    "DCApi",
		"10.64.38.0/24":    "SRS",
		"10.64.39.0/24":    "Metrika",
		"10.64.40.0/24":    "UKS",
		"10.64.41.0/24":    "EDI",
		"10.64.42.0/24":    "Queue",
		"10.64.43.0/24":    "Graphite",
		"10.64.44.0/24":    "Evrika-Phy",
		"10.64.45.0/24":    "International",
		"10.64.46.0/24":    "Market",
		"10.64.47.0/24":    "Admins-Svcs",
		"10.64.48.0/24":    "Vostok",
		"10.64.49.0/24":    "QuickCRM",
		"10.64.50.0/24":    "Intern",
		"10.64.51.0/24":    "Intern-13204",
		"10.64.53.0/24":    "DBAAS",
		"10.64.55.0/24":    "Pravo",
		"10.64.56.0/24":    "Travel",
		"10.64.57.0/24":    "BankGateway",
		"10.64.59.0/24":    "KR",
		"10.64.61.0/24":    "Sverka",
		"10.65.4.0/24":     "Elba",
		"10.65.5.0/24":     "Mercury",
		"10.65.6.0/24":     "Expert",
		"10.65.7.0/24":     "BG",
		"192.168.76.0/22":  "Diffs",
		"192.168.85.0/24":  "DevOps2",
		"192.168.86.0/24":  "Forms",
		"192.168.66.0/24":  "KOPF-sys-msk",
		"192.168.75.0/24":  "etc",
		"192.168.80.0/24":  "HypervisorsOLD",
		"192.168.83.0/24":  "MobileDev",
		"10.49.1.0/24":     "OIR",
		"10.49.2.0/24":     "KEInfra",
		"10.49.3.0/24":     "KEApi",
		"10.49.4.0/24":     "KEWeb",
		"10.49.5.0/24":     "Realty",
		"10.49.6.0/24":     "KS",
		"10.49.7.0/24":     "SRS",
		"10.49.8.0/24":     "Coza",
		"10.49.9.0/24":     "Elba",
		"10.49.10.0/24":    "DCApi",
		"10.49.11.0/24":    "Metrika",
		"10.49.12.0/24":    "UKS",
		"10.49.13.0/24":    "Queue",
		"10.49.15.0/24":    "Graphite",
		"10.49.16.0/24":    "KK",
		"10.49.17.0/24":    "International",
		"10.49.18.0/24":    "Evrika",
		"10.49.19.0/24":    "Market",
		"10.49.20.0/24":    "Retail1C",
		"10.49.21.0/24":    "AML",
		"10.49.23.0/24":    "Admins-Svcs",
		"10.49.24.0/24":    "Bank",
		"10.49.25.0/24":    "K84",
		"10.49.26.0/24":    "Mark",
		"10.49.27.0/24":    "QuickCRM",
		"10.49.28.0/24":    "EC",
		"10.49.29.0/24":    "XCOM",
		"10.49.30.0/24":    "Vostok-13076",
		"10.49.31.0/24":    "Billing",
		"10.49.32.0/24":    "MERCURY",
		"10.49.33.0/24":    "EXPERT",
		"10.49.34.0/24":    "BG",
		"10.49.35.0/24":    "Pravo",
		"10.49.37.0/24":    "Travel",
		"192.168.218.0/24": "VM-Forms",
		"192.168.219.0/24": "VM-Devops",
		"192.168.220.0/22": "VM-Diffs",
		"192.168.224.0/24": "Hypervisors",
		"192.168.226.0/24": "EDI",
		"192.168.228.0/24": "Focus",
		"192.168.230.0/24": "Dev-Hypervisors",
		"192.168.231.0/24": "etc",
		"192.168.248.0/24": "Kanso3D",
		"10.80.0.0/24":     "DHCP_RELAYs",
		"10.81.0.0/21":     "IPMIs",
		"10.81.13.0/24":    "SRS",
		"10.81.14.0/24":    "Hypervisors",
		"10.81.16.0/23":    "DDAPPS",
		"10.81.18.0/24":    "KR",
		"10.81.19.0/24":    "Admins-Svcs",
		"10.81.20.0/24":    "Market",
		"10.81.21.0/24":    "DEVOPS",
	}
	ProjectIPS["0.0.0.0"] = "AllNetworks"
	for netCidr, prjName := range ProjectIPBlocks {
		allHosts, err := Hosts(netCidr)
		if err != nil {
			log.Fatal(err)
		}
		for _, host := range allHosts {
			ProjectIPS[host] = prjName
		}
	}

}

func Hosts(cidr string) ([]string, error) {
	ip, ipnet, err := net.ParseCIDR(cidr)
	if err != nil {
		return nil, err
	}

	var ips []string
	for ip := ip.Mask(ipnet.Mask); ipnet.Contains(ip); inc(ip) {
		ips = append(ips, ip.String())
	}

	// remove network address and broadcast address
	lenIPs := len(ips)
	switch {
	case lenIPs < 2:
		return ips, nil

	default:
		return ips[1 : len(ips)-1], nil
	}
}

func inc(ip net.IP) {
	for j := len(ip) - 1; j >= 0; j-- {
		ip[j]++
		if ip[j] > 0 {
			break
		}
	}
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
