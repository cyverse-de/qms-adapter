package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/qms-adapter/amqp"
	"github.com/cyverse-de/qms-adapter/logging"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "main"})

// Configuration contains app-wide configuration settings.
type Configuration struct {
	QMSEnabled  bool
	QMSEndpoint *url.URL
}

// QMSUsageUpdate contains the fields needed to post updates
// to QMS.
type QMSUsageUpdate struct {
	Username             string  `json:"username"`
	ResourceType         string  `json:"resource_type"`
	UpdateType           string  `json:"update_type"`
	UsageAdjustmentValue float64 `json:"usage_adjustment_value"`
	EffectiveDate        string  `json:"effective_date"`
	Unit                 string  `json:"unit"`
}

const QMSUpdateTypeSet = "SET"

func getHandler(config *Configuration) amqp.HandlerFn {
	return func(update *amqp.QMSUpdate) {
		log = log.WithFields(logrus.Fields{"context": "update handler"})

		log.Debugf("QMS enabled: %v", config.QMSEnabled)

		if config.QMSEnabled {
			updateValue, err := strconv.ParseFloat(update.Value, 64)
			if err != nil {
				log.Error(err)
				return
			}

			putUpdate := &QMSUsageUpdate{
				Username:             update.Username,
				ResourceType:         update.Attribute,
				UpdateType:           QMSUpdateTypeSet,
				EffectiveDate:        time.Now().Format("2006-01-02"),
				Unit:                 update.Unit,
				UsageAdjustmentValue: updateValue,
			}

			resultBytes, err := json.Marshal(&putUpdate)
			if err != nil {
				log.Error(err)
				return
			}

			putBody := bytes.NewBuffer(resultBytes)

			updateRequest, err := http.NewRequest(http.MethodPut, config.QMSEndpoint.String(), putBody)
			if err != nil {
				log.Error(err)
				return
			}
			updateRequest.Header.Set("Content-Type", "application/json")

			putResp, err := http.DefaultClient.Do(updateRequest)
			if err != nil {
				log.Error(err)
				return
			}

			putRespBody, err := io.ReadAll(putResp.Body)
			if err != nil {
				log.Error(err)
				return
			}

			log.Infof("URL: %s, status code: %d, response: %s", putResp.Request.URL.String(), putResp.StatusCode, putRespBody)
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
