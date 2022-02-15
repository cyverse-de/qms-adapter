package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"net/http"
	"net/url"

	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/qms-adapter/amqp"
	"github.com/cyverse-de/qms-adapter/logging"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "main"})

type Configuration struct {
	QMSEnabled  bool
	QMSEndpoint *url.URL
}

func getHandler(config *Configuration) amqp.HandlerFn {
	return func(update *amqp.QMSUpdate) {
		log = log.WithFields(logrus.Fields{"context": "update handler"})

		log.Debugf("QMS enabled: %v", config.QMSEnabled)

		if config.QMSEnabled {
			resultBytes, err := json.Marshal(&update)
			if err != nil {
				log.Error(err)
				return
			}

			postBody := bytes.NewBuffer(resultBytes)

			postResp, err := http.Post(config.QMSEndpoint.String(), "application/json", postBody)
			if err != nil {
				log.Error(err)
				return
			}

			postRespBody, err := io.ReadAll(postResp.Body)
			if err != nil {
				log.Error(err)
				return
			}

			log.Infof("URL: %s, status code: %d, response: %s", postResp.Request.URL.String(), postResp.StatusCode, postRespBody)
		} else {
			log.Infof("%+v", update)
		}
	}
}

func main() {
	var (
		err    error
		config *viper.Viper

		configPath = flag.String("config", "/etc/iplant/de/jobservices.yml", "Full path to the configuration file")
		queue      = flag.String("queue", "qms-adapter", "The AMQP queue name for this service")
		reconnect  = flag.Bool("reconnect", false, "Whether the AMQP client should reconnect on failure")
		logLevel   = flag.String("log-level", "info", "One of trace, debug, info, warn, error, fatal, or panic")
		routingKey = flag.String("routing-key", "qms.usages", "The routing key for incoming AMQP messages")
	)

	flag.Parse()
	logging.SetupLogging(*logLevel)

	log.Infof("config path is %s", *configPath)

	config, err = configurate.Init(*configPath)
	if err != nil {
		log.Fatal(err)
	}

	amqpURI := config.GetString("amqp.uri")
	if amqpURI == "" {
		log.Fatal("amqp.uri must be set in the configuration file")
	}

	amqpExchange := config.GetString("amqp.exchange.name")
	if amqpExchange == "" {
		log.Fatal("amqp.exchange.name must be set in the configuration file")
	}

	amqpExchangeType := config.GetString("amqp.exchange.type")
	if amqpExchangeType == "" {
		log.Fatal("amqp.exchange.type must be set in the configuration file")
	}

	qmsEnabled := config.GetBool("qms.enabled")

	qmsBase := config.GetString("qms.base")
	if qmsEnabled && qmsBase == "" {
		log.Fatal("qms.base must be set if qms.enabled is true")
	}

	qmsUsage := config.GetString("qms.usage")
	if qmsEnabled && qmsUsage == "" {
		log.Fatal("qms.usage must be set if qms.enabled is true")
	}

	qmsEndpoint, err := url.Parse(qmsBase)
	if err != nil {
		log.Fatal(err)
	}

	qmsEndpoint.Path = qmsUsage

	configuration := Configuration{
		QMSEnabled:  qmsEnabled,
		QMSEndpoint: qmsEndpoint,
	}

	amqpConfig := amqp.Configuration{
		URI:           amqpURI,
		Exchange:      amqpExchange,
		ExchangeType:  amqpExchangeType,
		RoutingKey:    *routingKey,
		Reconnect:     *reconnect,
		Queue:         *queue,
		PrefetchCount: 0,
	}

	log.Infof("AMQP exchange name: %s", amqpConfig.Exchange)
	log.Infof("AMQP exchange type: %s", amqpConfig.ExchangeType)
	log.Infof("AMQP reconnect: %v", amqpConfig.Reconnect)
	log.Infof("AMQP queue name: %s", amqpConfig.Queue)
	log.Infof("AMQP prefetch amount %d", amqpConfig.PrefetchCount)
	log.Infof("AMQP routing key: %s", amqpConfig.RoutingKey)

	amqpClient, err := amqp.New(&amqpConfig, getHandler(&configuration))
	if err != nil {
		log.Fatal(err)
	}
	defer amqpClient.Close()

	log.Info("done connecting to the AMQP broker")

	select {}
}
