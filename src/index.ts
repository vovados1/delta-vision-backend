import { Hono } from "hono"
import { websocket } from "hono/bun"
import { InfluxDB } from "@influxdata/influxdb-client"
import { dataRoute } from "./routes/Data"

const HOSTNAME = String(process.env.HOSTNAME) || "localhost"
const PORT = Number(process.env.PORT) || 7000

const INFLUXDB_URL = String(process.env.INFLUXDB_URL)
const INFLUXDB_TOKEN = String(process.env.INFLUXDB_TOKEN)
const INFLUXDB_ORG_NAME = String(process.env.INFLUXDB_ORG_NAME)

const app = new Hono()

export const influxClient = new InfluxDB({ url: INFLUXDB_URL, token: INFLUXDB_TOKEN })
export const queryApi = influxClient.getQueryApi(INFLUXDB_ORG_NAME)

// Defining routes
app.route("/data", dataRoute)

export default {
  hostname: HOSTNAME,
  port: PORT,
  fetch: app.fetch,
  websocket,
}
