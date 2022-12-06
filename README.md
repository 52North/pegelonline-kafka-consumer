# PEGELONLINE Kafka Consumer
The PEGELONLINE Kafka Consumer is an intermediary layer, which reads streams of PEGELONLINE measurements and forwards the messages to certain RabbitMQ MQTT topics.

## Get started
### Build the project
Build the project with Maven: `mvn clean install`

### Service Dependencies
Make sure the services listed below are running locally:  
* Kafka: localhost:9092  
* RaqbbitMQ: localhost:1883

For testing purposes, the project comes with a [docker-compose.yml](./docker/docker-compose.yml) for setting up a Kafka and RabbitMQ service locally via Docker.

### Start the Consumer
Run the PEGELONLINE Kafka Consumer by typing: `mvn spring-boot:run`.  

For testing purposes, the consumer will start a tiny web server under `localhost:9000` and provide a single endpoint `http://localhost:9000/send/messages` for sending dummy messages. Sending a message to it will trigger the workflow listed below:
1) The dummy message will be published via Kafka.
2) A Kafka consumer receives the messages.
3) The message payload will be processed and an MQTT topic extracted.
4) The message will be forwarded to RabbitMQ under the extracted topic

Use the cURL command listed below to send a dummy message to the consumer service.

```
curl --location --request POST 'http://localhost:9000/send/messages' \
--header 'Content-Type: application/json' \
--data-raw '{
  "uuid": "593647aa-9fea-43ec-a7d6-6476a76ae868",
  "number": "2710080",
  "shortname": "BONN",
  "longname": "BONN",
  "km": 654.8,
  "agency": "STANDORT DUISBURG",
  "longitude": 7.108045442593695,
  "latitude": 50.73639800887676,
  "water": {
    "shortname": "RHEIN",
    "longname": "RHEIN"
  },
  "timeseries": [
    {
      "shortname": "W",
      "longname": "WASSERSTAND ROHDATEN",
      "unit": "cm",
      "equidistance": 15,
      "currentMeasurement": {
        "timestamp": "2022-07-18T16:45:00+02:00",
        "value": 124.0,
        "trend": -1,
        "stateMnwMhw": "normal",
        "stateNswHsw": "unknown"
      },
      "gaugeZero": {
        "unit": "m. ü. NHN",
        "value": 42.713,
        "validFrom": "2019-11-01"
      }
    },
    {
      "shortname": "Q",
      "longname": "ABFLUSS_ROHDATEN",
      "unit": "m³/s",
      "equidistance": 15,
      "currentMeasurement": {
        "timestamp": "2022-07-18T16:15:00+02:00",
        "value": 801.0,
        "trend": -1
      }
    }
  ]
}'
```
