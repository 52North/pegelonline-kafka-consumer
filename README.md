# PEGELONLINE Kafka Consumer
The PEGELONLINE Kafka Consumer is an intermediary layer, which reads streams of PEGELONLINE measurements and forwards
the messages to certain RabbitMQ MQTT topics.

## Get started
### Build the project
Build the project with Maven: `mvn clean install`

### Service Dependencies
Make sure the services listed below are running locally:  
* Kafka: localhost:9092  
* RaqbbitMQ: localhost:1883

For testing purposes, the project comes with a [docker-compose.yml](./docker/docker-compose.yml) for setting up a Kafka
and RabbitMQ service locally via Docker. Just run `docker compose up`

#### TLS Support
In order to use TLS for connecting to RabbitMQ, the project also comes with a separate Docker Compose configuration.
[docker-compose.tls.yml](./docker/docker-compose.tls.yml) inherits the default [docker-compose.yml](./docker/docker-compose.yml)
by some extra configurations:
* Certificate files are mounted into the RabbitMQ container (which are expected to be placed in the [./docker/certs](./docker/certs) folder)
* Port 8883 will be opened for TLS connections
* A [rabbitmq.conf](./docker/rabbitmq.conf), which has appropriate config definitions for TLS support is provided to the RabbitMQ container.

Start this setup by executing `docker compose -f .\docker-compose.yml -f .\docker-compose.tls.yml up`

You can read more about TLS support for RabbitMQ and how to generate appropriate certificates for testing purpose in
the [RabbitMQ documentation](https://www.rabbitmq.com/ssl.html).

### Start the Consumer
Run the PEGELONLINE Kafka Consumer by typing: `mvn spring-boot:run`.  

For testing purposes, the consumer will start a tiny web server under `localhost:9000` and provide a single endpoint
`http://localhost:9000/send/messages` for sending dummy messages. Sending a message to it will trigger the workflow
listed below:
1) The dummy message will be published via Kafka.
2) A Kafka consumer receives the messages.
3) The message payload will be processed and an MQTT topic extracted.
4) The message will be forwarded to RabbitMQ under the extracted topic

Use the cURL command listed below to send a dummy message to the consumer service. You'll find an example payload at
[./docs/pegelonline-kafka-message-example.json](./docs/pegelonline-kafka-message-example.json). Just replace the
`<payload>` placeholder in the cURL command with its content. 

```
curl --location --request POST 'http://localhost:9000/send/messages' \
--header 'Content-Type: application/json' \
--data-raw '<payload>'
```

### Run in Production
To run the PEGELONLINE Kafka Consumer in production it is strongly recommended to use TLS for connecting to RabbitMQ.
To do so use SSL protocol and port for the MQTT connection URI, e.g.:
`edis.mqtt.server-uri=ssl://<remote-server-uri>:8883` 

You also have to provide a trusted server CA certificate as well as client certificate and private key. Each
should be in PEM format. You can set the file paths as well as other TLS related parameters via externalized properties:

* `edis.mqtt.tls.tls-enabled`: Set to `true` if you want to use TLS enabled MQTT connections
* `edis.mqtt.tls.peer-verification-enabled`: Set to `true` if you want to enable [TLS peer verification](https://www.rabbitmq.com/ssl.html#peer-verification) 
* `edis.mqtt.tls.ca-cert-file`: Path to the server CA cert file in PEM format
* `edis.mqtt.tls.client-cert-file`: Path to the client cert file in PEM format
* `edis.mqtt.tls.key-file`: Path to the client key file in PEM format
* `edis.mqtt.tls.password`: Password for the client key

### Deploy
For deploying a release to Nexus you can use the [Maven Release Plugin](https://maven.apache.org/maven-release/maven-release-plugin/index.html)
by performing the steps listed below:
1. Create a `<server>...</server>` entry within your local *.m2/settings.xml*:
```xml
<settings>
    <servers>
        <server>
            <id>${repo_id}</id>
            <username>${repo_user}</username>
            <password>${repo_pw}</password>
        </server>
    </servers>
</settings>
```
2. Execute the _prepare_ goal: `mvn release:prepare`. If you do not run in batch mode you will be interactively asked
for version tags. The _prepare_ goal will tag your latest commit with the specified version label and pushes it to GitLab.
3. Execute the _perform_ goal. You have to specify the `repo_id`, `repo_user` and `repo_pw` in your command:
`mvn -Drepoid=nexus-releases -Drepo_user=user -Drepo_pw=password release:perform`. Only change `repo_user` and `repo_pw`!
The _perform_ goal will release the latest artifact to Nexus.

Note: If you aim to perform the release as part of your CI-pipeline e.g. within a Jenkins build environment, you also
have to set Git credentials with write access. For this purpose, use the _jenkins-release_ profile, which uses
`SCM_USERNAME` and `SCM_PASSWORD` environment variables, which you have to set in beforehand.

#### Deploy Snapshot
For testing purposes you could also deploy a local snapshot build to the Nexus Snapshots repository:
1. Build the project per `mvn clean install`
2. Deploy the *-SNAPSHOT.jar artifact: `mvn -Drepoid=nexus-snapshot -Drepo_user=user -Drepo_pw=password deploy`