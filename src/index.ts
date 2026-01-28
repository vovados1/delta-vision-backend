import { Hono } from "hono"
import { websocket } from "hono/bun"
import { dataRoute } from "./routes/Data"

const HOSTNAME = String(process.env.HOSTNAME) || "localhost"
const PORT = Number(process.env.PORT) || 7000

const app = new Hono()

// Defining routes
app.route("/data", dataRoute)

export default {
  hostname: HOSTNAME,
  port: PORT,
  fetch: app.fetch,
  websocket,
}
