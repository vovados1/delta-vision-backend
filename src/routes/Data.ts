import { Hono } from "hono"
import { upgradeWebSocket } from "hono/bun"
import { queryApi } from ".."

export const dataRoute = new Hono()

const INFLUXDB_BUCKET_NAME = String(process.env.INFLUXDB_BUCKET_NAME)

dataRoute.get(
  "/",
  upgradeWebSocket(() => {
    const fluxQuery = `
    from(bucket: "${INFLUXDB_BUCKET_NAME}")
      |> range(start: -1d)
      |> filter(fn: (r) => r["_measurement"] == "Binance" or r["_measurement"] == "ByBit" or r["_measurement"] == "Coinbase" or r["_measurement"] == "Kraken" or r["_measurement"] == "Okx")
      |> filter(fn: (r) => r["pair"] == "btc/usdt")
      |> filter(fn: (r) => r["_field"] == "ask")
      |> yield(name: "mean")
  `

    return {
      async onOpen(_, ws) {
        for await (const { values, tableMeta } of queryApi.iterateRows(fluxQuery)) {
          const o = tableMeta.toObject(values)
          ws.send(JSON.stringify({ data: o }))
        }
      },
      onClose: () => console.log("Connection closed"),
    }
  })
)
