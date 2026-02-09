import { Hono } from "hono"
import { upgradeWebSocket } from "hono/bun"
import { KafkaClient } from "../clients/KafkaClient"
import { WSClient } from "../clients/WSClient"

export const dataRoute = new Hono()

const kafkaClient = new KafkaClient(
  String(process.env.KAFKA_CLIENT_ID),
  String(process.env.KAFKA_BROKERS).split(","),
  String(process.env.KAFKA_GROUP_ID),
  String(process.env.KAFKA_TOPIC_NAME)
)

const wsClient = new WSClient(kafkaClient)

dataRoute.get(
  "/",
  upgradeWebSocket(() => {
    return {
      onOpen(evt, ws) {
        wsClient.open(ws)
      },
      onMessage(event, ws) {
        const msg = JSON.parse(event.data.toString())
        wsClient.handleMessage(msg, ws)
      },
      onClose(_, ws) {
        wsClient.close(ws)
      },
    }
  })
)
