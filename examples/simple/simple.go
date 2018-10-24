package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"strings"
	"sync"
	"time"

	ac "github.com/dropbox/load_management/admission_control"
	"github.com/dropbox/load_management/load_manager"
	"github.com/dropbox/load_management/scorecard"
)

const (
	FooHandlerName = "Foo"
	BarHandlerName = "Bar"
)

var loadManager *load_manager.LoadManager
var loadManagerOnce sync.Once
var mainQueueName = load_manager.QueueName("main")

func LoadManager() *load_manager.LoadManager {
	loadManagerOnce.Do(func() {
		loadManager = load_manager.NewLoadManager(
			map[load_manager.QueueName]ac.AdmissionController{
				mainQueueName: ac.NewAdmissionController(1),
			},
			ac.NewAdmissionController(1),
			scorecard.NewScorecard(scorecard.NoRules()),
			nil,                // No canary scorecard
			scorecard.NoTags(), // No default tags.
		)
	})
	return loadManager
}

func makeTag(prefix string, suffix interface{}) scorecard.Tag {
	return scorecard.Tag(fmt.Sprintf("%s:%v", prefix, suffix))
}

// Foo reports the time spent in the request and uses load manager.
func Foo(ctx context.Context, conn net.Conn, data []byte) error {
	start := time.Now()

	maybeHostPort := conn.RemoteAddr().String()
	host, _, _ := net.SplitHostPort(maybeHostPort)
	requestSize := int(math.Log10(float64(len(data))))
	tags := []scorecard.Tag{
		makeTag("handler", FooHandlerName),
		makeTag("source_ip", host),
		makeTag("request_size", requestSize),
	}
	resource := LoadManager().GetResource(ctx, mainQueueName, tags)
	if !resource.Acquired() {
		return fmt.Errorf("Error acquiring resource")
	}
	defer resource.Release()

	msg := fmt.Sprintf("Request took %f ms\n", time.Since(start).Seconds()*1000)
	_, err := conn.Write([]byte(msg))
	if err != nil {
		return err
	}
	return nil
}

// Bar echoes the request and has no limiting.
func Bar(ctx context.Context, conn net.Conn, data []byte) error {
	_, err := conn.Write(data)
	return err
}

func handleClient(ctx context.Context, conn net.Conn) {
	b := bufio.NewReader(conn)
	for {
		line, err := b.ReadBytes('\n')
		if err != nil {
			log.Println(err.Error())
			break
		}
		cmd := strings.TrimSpace(string(line))
		switch cmd {
		case FooHandlerName:
			err = Foo(ctx, conn, line)
		case BarHandlerName:
			err = Bar(ctx, conn, line)
		default:
			_, err = conn.Write([]byte(fmt.Sprintf("Unknown command '%s'\n", cmd)))
		}
		if err != nil {
			log.Println(err.Error())
			break
		}
	}
}

// Run with:
//   go build
//   ./examples --port 8080
//
// Client:
//   nc 127.0.0.1 8080
//   Foo
//   Request took 0.084198 ms
//   Foo
//   Request took 0.048530 ms
//   bar
//   Unknown command 'bar'
func main() {
	flagIP := flag.String("ip", "127.0.0.1", "ip on which to listen")
	flagPort := flag.Int("port", 0, "port on which to listen")
	flag.Parse()

	addr := fmt.Sprintf("%s:%d", *flagIP, *flagPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println(err.Error())
			break
		} else {
			go handleClient(ctx, conn)
		}
	}
}
