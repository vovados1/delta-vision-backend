import { Hono } from "hono"
import { upgradeWebSocket } from "hono/bun"
import { KafkaClient } from "../clients/KafkaClient"
import { WSClient } from "../clients/WSClient"
import { DataController } from "../controllers/Data"

export const dataRoute = new Hono()

const kafkaClient = new KafkaClient(
  String(process.env.KAFKA_CLIENT_ID),
  String(process.env.KAFKA_BROKERS).split(","),
  String(process.env.KAFKA_GROUP_ID),
  String(process.env.KAFKA_TOPIC_NAME)
)

const wsClient = new WSClient(kafkaClient)

dataRoute.get("/stream", upgradeWebSocket(DataController.stream(wsClient)))
dataRoute.get("/meta", DataController.meta(wsClient))
