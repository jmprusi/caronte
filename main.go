// goronte project main.go
package main

import (
	"encoding/json"
	"fmt"
	"github.com/codegangsta/cli"
	"github.com/garyburd/redigo/redis"
	"github.com/streadway/amqp"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type caronteMessage struct {
	Exchange   string
	Routingkey string
}

type Config struct {
	AmqpURI           string
	RedisHost         string
	RedisList         string
	JSONKey           string
	Debug             bool
	Workers           int
	GraveyardExchange string
	GraveyardFile     string
	LogFile           string
}

func main() {
	app := cli.NewApp()
	app.Name = "Caronte"
	app.Usage = "Redis 2 RabbitMQ Transporter"
	app.Flags = []cli.Flag{
		cli.StringFlag{"rabbitmq_host", "localhost", "RabbitMQ server host", "rabbitmq_host"},
		cli.StringFlag{"rabbitmq_user", "guest", "RabbitMQ user", "rabbitmq_user"},
		cli.StringFlag{"rabbitmq_pass", "guest", "RabbitMQ password", "rabbitmq_password"},
		cli.StringFlag{"redis_host", "localhost", "Redis server host", "redis_host"},
		cli.BoolFlag{"debug", "Print debug info stdout", "caronte_debug"},
		cli.StringFlag{"redis_list", "caronte", "Redis list", "redis_list"},
		cli.StringFlag{"jsonkey", "caronte", "JSON Key to look for on msgs.", "jsonkey"},
		cli.IntFlag{"workers", 5, "Number of workers", "caronte_workers"},
		cli.StringFlag{"graveyard_exchange", "graveyard", "Name of the graveyard exchange on rabbitmq ", "graveyard_exchange"},
		cli.StringFlag{"graveyard_file", "graveyard", "Name of the graveyard file for dumping msgs when there's no rabbitmq graveyard", "graveyard_file"},
	}
	app.Action = func(c *cli.Context) {
		runApp(c)
	}
	app.Run(os.Args)
}

func runApp(c *cli.Context) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.Println("> Starting Redis Client")

	config := Config{
		"amqp://" + c.String("rabbitmq_user") + ":" + c.String("rabbitmq_pass") + "@" + c.String("rabbitmq_host") + ":5672",
		c.String("redis_host"),
		c.String("redis_list"),
		c.String("jsonkey"),
		c.Bool("debug"),
		c.Int("workers"),
		c.String("graveyard_exchange"),
		c.String("graveyar_file"),
		"output",
	}

	for i := 1; i <= config.Workers; i++ {
		go getMessages(config, i)
	}
	var input string
	fmt.Scanln(&input)
}

func connectRedis(c Config) redis.Conn {
	r, err := redis.Dial("tcp", c.RedisHost+":6379")
	if err != nil {
		panic(err)
	}
	return r
}

func getMessages(c Config, i int) {
	r := connectRedis(c)
	s := []string{"stage", strconv.Itoa(i)}
	stage := strings.Join(s, "_")
	for {

		v, _ := r.Do("RPOPLPUSH", c.RedisList, strings.Join(s, "_"))

		if v == nil {
		}

		reply, err := redis.Values(r.Do("LRANGE", stage, "0", "0"))

		if err != nil || len(reply) < 1 {

			time.Sleep(2 * time.Second) // totally made up waiting time..

		} else {
			var objmap map[string]*json.RawMessage
			err = json.Unmarshal(reply[0].([]byte), &objmap)
			if objmap[c.JSONKey] != nil {
				j, _ := json.Marshal(objmap[c.JSONKey])
				var metadata caronteMessage
				err := json.Unmarshal(j, &metadata)
				if err != nil {
					panic(err)
				}
				delete(objmap, c.JSONKey)
				messageClean, _ := json.Marshal(objmap)
				publishMessage(messageClean, metadata, c)
				r.Do("LPOP", stage)
			} else {
				log.Println("Not a valid message!")
				r.Do("LPOP", stage)
			}
		}
	}
}

func publishMessage(messageClean []byte, metadata caronteMessage, c Config) {
	connection, err := amqp.Dial(c.AmqpURI)
	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		fmt.Errorf("Channel: %s", err)
	}

	if err = channel.Publish(
		metadata.Exchange,
		metadata.Routingkey,
		true,
		false,
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "UTF-8",
			Body:            []byte(messageClean),
			DeliveryMode:    amqp.Persistent,
			Priority:        0,
		},
	); err != nil {
		fmt.Errorf("Exchange Publish: %s", err)
	}

}
