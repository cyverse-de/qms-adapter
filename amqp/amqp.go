package amqp

import (
	"context"
	"encoding/json"

	"github.com/cyverse-de/messaging/v9"
	"github.com/cyverse-de/qms-adapter/logging"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "amqp"})

// Configuration contains the AMQP settings.
type Configuration struct {
	URI           string
	Reconnect     bool
	Exchange      string
	ExchangeType  string
	RoutingKey    string
	Queue         string
	PrefetchCount int
}

// QMSUpdate contains the information sent to the QMS service.
type QMSUpdate struct {
	Attribute string `json:"attribute"`
	Value     string `json:"value"`
	Unit      string `json:"unit"`
	UserID    string `json:"user_id"`
	Username  string `json:"username"`
}

// HandlerFn is the function signature for QMS update handlers.
type HandlerFn func(context.Context, *QMSUpdate)

// AMQP encapsulates the logic for handling AMQP messages.
type AMQP struct {
	client  *messaging.Client
	handler HandlerFn
}

// New returns a new *AMQP based on the configuration and HandlerFn passed in.
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

func (a *AMQP) recv(ctx context.Context, delivery amqp.Delivery) {
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

	a.handler(ctx, &update)
}

// Close closes the connection to the AMQP broker.
func (a *AMQP) Close() {
	a.client.Close()
}
