import { Hono } from "hono"
import { upgradeWebSocket } from "hono/bun"
import type { WSContext } from "hono/ws"
import { Kafka } from "kafkajs"

export const dataRoute = new Hono()

const KAFKA_TOPIC = String(process.env.KAFKA_TOPIC_NAME)

export const kafkaClient = new Kafka({
  clientId: String(process.env.KAFKA_CLIENT_ID),
  brokers: String(process.env.KAFKA_BROKERS).split(","),
})

// Map: key (e.g., "binance_usdt/btc") -> Set of WebSocket connections
const subscriptions = new Map<string, Set<WSContext>>()

const consumer = kafkaClient.consumer({ groupId: "delta-vision-backend-consumer" })

await consumer.connect()
await consumer.subscribe({ topic: KAFKA_TOPIC })

consumer.run({
  async eachMessage({ message }) {
    const key = message.key?.toString()
    if (!key) return

    const clients = subscriptions.get(key)
    if (clients?.size) {
      const payload = JSON.stringify({
        key,
        value: message.value?.toString(),
      })
      for (const ws of clients) {
        ws.send(payload)
      }
    }
  },
})

function subscribe(ws: WSContext, key: string) {
  if (!subscriptions.has(key)) {
    subscriptions.set(key, new Set())
  }
  subscriptions.get(key)!.add(ws)
}

function unsubscribe(ws: WSContext, key: string) {
  subscriptions.get(key)?.delete(ws)
}

function unsubscribeAll(ws: WSContext) {
  for (const clients of subscriptions.values()) {
    clients.delete(ws)
  }
}

dataRoute.get(
  "/",
  upgradeWebSocket(() => {
    return {
      onMessage(event, ws) {
        const msg = JSON.parse(event.data.toString())
        // { action: "subscribe", key: "binance_usdt/btc" }
        // { action: "unsubscribe", key: "binance_usdt/btc" }
        if (msg.action === "subscribe") {
          subscribe(ws, msg.key)
        } else if (msg.action === "unsubscribe") {
          unsubscribe(ws, msg.key)
        }
      },
      onClose(_, ws) {
        unsubscribeAll(ws)
      },
    }
  })
)
