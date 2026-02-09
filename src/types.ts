export interface ConnectionData {
  exchanges: string[]
  pairs: string[]
  refreshRate: "1s" | "2s" | "5s" | "10s" | null
}

export interface ConnectionConfig extends ConnectionData {
  id: string
  // Epoch timestamp
  lastMessageReceived: number
}

export type WSMessage = ({ type: "join_pool" } & ConnectionData) | { type: "leave_pool" }

export interface KafkaPayload {
  key: string
  data: {
    bid: number
    ask: number
    bidQty: number
    askQty: number
    timestamp: number
  }
}
