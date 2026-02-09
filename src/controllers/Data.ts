import type { Context } from "hono"
import type { WSEvents } from "hono/ws"
import type { WSClient } from "../clients/WSClient"
import type { H } from "hono/types"

const stream =
  (wsClient: WSClient) =>
  (_: Context): WSEvents => ({
    onOpen(_, ws) {
      wsClient.open(ws)
    },
    onMessage(event, ws) {
      const msg = JSON.parse(event.data.toString())
      wsClient.handleMessage(msg, ws)
    },
    onClose(_, ws) {
      wsClient.close(ws)
    },
  })

const meta =
  (wsClient: WSClient): H =>
  (c) =>
    c.json(wsClient.metadata)

export const DataController = { stream, meta }
