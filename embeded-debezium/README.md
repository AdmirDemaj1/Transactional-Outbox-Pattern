# Change Data Capture (CDC) with Embedded Debezium and SpringBoot

Blog URL: https://medium.com/@sohan_ganapathy/change-data-capture-cdc-with-embedded-debezium-and-springboot-6f10cd33d8ec

## Prerequisites
- [Docker](https://docs.docker.com/v17.09/engine/installation/)
- [Docker Compose](https://docs.docker.com/compose/install/)
- [Nestjs]


## Starting docker images

Once the prerequisites are installed, run the command.

In `embeded-debezium`:

```shell
export DEBEZIUM_VERSION=2.4
export DEBEZIUM_CONNECTOR_VERSION=2.4.0.Alpha2
docker-compose up -d
```

## Starting the SpringBoot application

Go into student-cdc-relay folder and run

```shell
mvn spring-boot:run
```


## Starting the Nestjs Application

1. Navigate into `debezium-endpoints`
2. Run:
```shell
npm install -g @nestjs/cli
nest --version
npm run start
```
3. Install `REST Client` from extensions to test the endpoint
4. Run `npm run start`
5. Test the endpoint into rest-client.http by clicking Send Request

## Check if everything worked

1. Go into your local pgAdmin and check if a new record was added into studentdb -> public tables -> students
2. Open `http://localhost:9000/topic/student-topic` or `http://localhost:9000/` and you should see the umber of created messages there
3. Try to call the request multiple times and it should generate a new message for each successful request

