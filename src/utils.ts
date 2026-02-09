import type { ConnectionData } from "./types"

export const generateKey = (exchange: string, pair: string) => `${exchange}_${pair}`

export const separateKey = (key: string) => {
  const [exchange, pair] = key.split("_")
  return { exchange: String(exchange), pair: String(pair) }
}

export const hasTimeElapsedFrom = (timestamp: number, variant: NonNullable<ConnectionData["refreshRate"]>) => {
  const now = Date.now()

  const variants: Record<NonNullable<ConnectionData["refreshRate"]>, number> = {
    "1s": 1000,
    "2s": 2000,
    "5s": 5000,
    "10s": 10000,
  }

  return now - variants[variant] > timestamp
}
