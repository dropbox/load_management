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
	sleepHandlerName = "Sleep"
)

const mainQueueName = "main"

var loadManager *load_manager.LoadManager
var loadManagerOnce sync.Once

// LoadManager lazily initialize global load manager with default value and returns it.
func LoadManager() *load_manager.LoadManager {
	loadManagerOnce.Do(func() {
		loadManager = load_manager.NewLoadManager(
			map[string]ac.AdmissionController{
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

// Sleep reports the time spent in the request and uses load manager.
func Sleep(ctx context.Context, conn net.Conn, data []byte) error {
	start := time.Now()

	maybeHostPort := conn.RemoteAddr().String()
	host, _, _ := net.SplitHostPort(maybeHostPort)
	requestSize := int(math.Log10(float64(len(data))))
	tags := []scorecard.Tag{
		makeTag("handler", sleepHandlerName),
		makeTag("source_ip", host),
		makeTag("request_size", requestSize),
	}
	resource := LoadManager().GetResource(ctx, mainQueueName, tags)
	if !resource.Acquired() {
		msg := "Too many sleepers! Please come back later!\n"
		_, _ = conn.Write([]byte(msg))
		return fmt.Errorf("Error acquiring resource")
	}
	defer resource.Release()

	msg := fmt.Sprintf("Request took %s\n", time.Since(start))
	_, err := conn.Write([]byte(msg))
	if err != nil {
		return err
	}
	return nil
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
		case sleepHandlerName:
			err = Sleep(ctx, conn, line)
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
// WIth a single client:
//   nc 127.0.0.1 8080
//   Sleep
//   Request took 10.0084198s
//   Sleep
//   Request took 10.00048530s
//
// If you try running the above in multiple clients, you will eventually hit
// this case:
//   nc 127.0.0.1 8080
//	 Sleep
//   Too many sleepers! Please come back later!
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
