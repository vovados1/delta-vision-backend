/*
  options = {
    exchanges: string[]
    pairs: string[]
    refreshRate: "1s" | "2s" | "5s" | "10s"
  }

  -> connect through WebSocket (do nothing)
  -> handle `onMessage`: { action: string, ...options }
    -> { action: "subscribe", ...options }
    -> { action: "unsubscribe" }
  -> handle `onClose` - unsubscribe
  -> only one subscription to kafka
  -> each ws connection is uniqely identified
*/

import { randomUUIDv7, type ServerWebSocket } from "bun"
import type { WSContext } from "hono/ws"
import type { ConnectionConfig, ConnectionData, KafkaPayload, WSMessage } from "../types"
import type { KafkaClient } from "./KafkaClient"
import { generateKey, hasTimeElapsedFrom, separateKey } from "../utils"

export class WSClient {
  protected readonly connections = new Map<ServerWebSocket, ConnectionConfig>()

  constructor(protected readonly kafkaClient: KafkaClient) {}

  get metadata() {
    return this.kafkaClient.allKeys.reduce<{ exchanges: string[]; pairs: string[] }>(
      (acc, key) => {
        const { exchange, pair } = separateKey(key)

        if (!acc.exchanges.includes(exchange)) acc.exchanges.push(exchange)
        if (!acc.pairs.includes(pair)) acc.pairs.push(pair)

        return acc
      },
      {
        exchanges: [],
        pairs: [],
      }
    )
  }

  open(ws: WSContext) {
    const wsRaw = ws.raw as ServerWebSocket

    this.connections.set(wsRaw, {
      id: randomUUIDv7(),
      exchanges: [],
      pairs: [],
      refreshRate: null,
      lastMessageReceived: 0,
    })
  }

  close(ws: WSContext) {
    const wsRaw = ws.raw as ServerWebSocket
    const config = this.connections.get(wsRaw)
    if (!config) throw new Error("Config not found for WS connection")

    config.exchanges.forEach((exchange) =>
      config.pairs.forEach((pair) => this.kafkaClient.unsubscribe(config.id, generateKey(exchange, pair)))
    )
    this.connections.delete(wsRaw)
  }

  handleMessage(message: WSMessage, ws: WSContext) {
    const { type, ...data } = message
    const wsRaw = ws.raw as ServerWebSocket
    const config = this.connections.get(wsRaw)
    if (!config) throw new Error("Config not found for WS connection")

    if (type === "join_pool") {
      const newConfig = { ...config, ...(data as ConnectionData) }
      this.connections.set(wsRaw, newConfig)
      newConfig.exchanges.forEach((exchange) =>
        newConfig.pairs.forEach((pair) => {
          this.kafkaClient.subscribe(newConfig.id, generateKey(exchange, pair), this.onKafkaMessage)
        })
      )
    } else if (type === "leave_pool") {
      this.connections.set(wsRaw, {
        ...config,
        exchanges: [],
        pairs: [],
        refreshRate: null,
        lastMessageReceived: 0,
      })
    }
  }

  protected onKafkaMessage = (payload: KafkaPayload) => {
    const { exchange, pair } = separateKey(payload.key)

    for (const [ws, config] of this.connections) {
      if (
        config.exchanges.includes(exchange) &&
        config.pairs.includes(pair) &&
        config.refreshRate !== null &&
        hasTimeElapsedFrom(config.lastMessageReceived, config.refreshRate)
      ) {
        this.connections.set(ws, { ...config, lastMessageReceived: Date.now() })
        ws.send(JSON.stringify(payload))
      }
    }
  }
}
