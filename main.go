// goronte project main.go
package main

import (
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/streadway/amqp"
	"log"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type CaronteMessage struct {
	Exchange    string
	Routing_key string
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.Println("> Starting Redis Client")
	go stageMessages()
	for i := 1; i <= 20; i++ {
		go getMessages(i)
	}
	var input string
	fmt.Scanln(&input)
}

func connectRedis() redis.Conn {
	r, err := redis.Dial("tcp", ":6379")
	if err != nil {
		panic(err)
	}
	return r
}

func stageMessages() {
	r := connectRedis()
	for {
		for i := 1; i <= 20; i++ {
			s := []string{"stage", strconv.Itoa(i)}
			v, _ := r.Do("RPOPLPUSH", "caronte", strings.Join(s, "_"))
			if v == nil {
				log.Printf("stager Sleeping!\n")
				time.Sleep(1 * time.Second) // totally made up waiting time..
			}
		}
	}
	r.Close()
}

func getMessages(i int) {
	r := connectRedis()
	s := []string{"stage", strconv.Itoa(i)}
	stage := strings.Join(s, "_")

	for {

		reply, err := redis.Values(r.Do("LRANGE", stage, "0", "0"))

		if err != nil || len(reply) < 1 {

			log.Printf("getMessage Sleeping!\n")
			time.Sleep(2 * time.Second) // totally made up waiting time..

		} else {

			log.Printf("Got message!\n")

			var objmap map[string]*json.RawMessage
			err = json.Unmarshal(reply[0].([]byte), &objmap)
			if objmap["caronte"] != nil {
				publishMessage(objmap)
			} else {
				log.Println("Not a valid message!\n")
				r.Do("LPOP", stage)
			}
		}
	}
	r.Close()
}

func publishMessage(objmap map[string]*json.RawMessage) {
	amqpURI := "amqp://guest:guest@localhost:5672/"

	j, _ := json.Marshal(objmap["caronte"])
	var metadata CaronteMessage
	err := json.Unmarshal(j, &metadata)

	log.Printf("exchange %s routing_key %s", metadata.Exchange, metadata.Routing_key)

	delete(objmap, "caronte")
	messageClean, _ := json.Marshal(objmap)
	log.Printf("body: %s\n", messageClean)

	log.Printf("dialing %q", amqpURI)
	connection, err := amqp.Dial(amqpURI)

	if err != nil {
		panic(err)
	}

	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		fmt.Errorf("Channel: %s", err)
	}

	if err = channel.Publish(
		metadata.Exchange,    // publish to an exchange
		metadata.Routing_key, // routing to 0 or more queues
		false,                // mandatory
		false,                // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            []byte(messageClean),
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
			// a bunch of application/implementation-specific fields
		},
	); err != nil {
		fmt.Errorf("Exchange Publish: %s", err)
	}

}
