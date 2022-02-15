package amqp

import (
	"encoding/json"

	"github.com/cyverse-de/qms-adapter/logging"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"gopkg.in/cyverse-de/messaging.v6"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "amqp"})

type Configuration struct {
	URI           string
	Reconnect     bool
	Exchange      string
	ExchangeType  string
	RoutingKey    string
	Queue         string
	PrefetchCount int
}

type QMSUpdate struct {
	Attribute string `json:"attribute"`
	Value     string `json:"value"`
	Unit      string `json:"unit"`
}

type HandlerFn func(*QMSUpdate)

type AMQP struct {
	client  *messaging.Client
	handler HandlerFn
}

func New(config *Configuration, handler HandlerFn) (*AMQP, error) {
	log.Debug("creating a new AMQP client")
	client, err := messaging.NewClient(config.URI, config.Reconnect)
	if err != nil {
		return nil, err
	}
	log.Debug("done creating a new AMQP client")

	a := &AMQP{
		client:  client,
		handler: handler,
	}

	go a.client.Listen()

	log.Debug("adding a consumer")
	client.AddConsumer(
		config.Exchange,
		config.ExchangeType,
		config.Queue,
		config.RoutingKey,
		a.recv,
		config.PrefetchCount,
	)
	log.Debug("done adding a consumer")

	return a, err
}

func (a *AMQP) recv(delivery amqp.Delivery) {
	var (
		update QMSUpdate
		err    error
	)

	log.Debugf(
		"message received; exchange: %s, routing key: %s, body: %s",
		delivery.Exchange,
		delivery.RoutingKey,
		string(delivery.Body),
	)

	if err = delivery.Ack(false); err != nil {
		log.Error(err)
		return
	}

	redelivered := delivery.Redelivered
	if err = json.Unmarshal(delivery.Body, &update); err != nil {
		log.Error(err)
		if err = delivery.Reject(!redelivered); err != nil {
			log.Error(err)
		}
		return
	}

	a.handler(&update)
}

func (a *AMQP) Listen() {
	a.client.Listen()
}

func (a *AMQP) Close() {
	a.client.Close()
}
