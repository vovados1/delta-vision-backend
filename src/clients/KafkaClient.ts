import { Kafka, type Consumer } from "kafkajs"
import type { KafkaPayload } from "../types"

export class KafkaClient {
  private readonly client: Kafka | null
  private readonly consumer: Consumer | null
  // { [key]: [[id, fn], ...] }
  private subscriptions: Map<string, [string, (payload: KafkaPayload) => unknown][]> = new Map()

  constructor(
    private readonly clientId: string,
    private readonly brokers: string[],
    private readonly groupId: string,
    private readonly topic: string
  ) {
    this.client = new Kafka({ clientId, brokers })
    this.consumer = this.client.consumer({ groupId })
    this.init()
  }

  protected async init() {
    if (!this.consumer) return
    await this.consumer.connect()
    await this.consumer.subscribe({ topic: this.topic })

    this.consumer.run({
      eachMessage: async ({ message }) => {
        try {
          const key = message.key?.toString()
          const value = message.value?.toString()
          if (!key || !value) return

          const subscribers = this.subscriptions.get(key)
          if (!subscribers) return

          const parsedValue = JSON.parse(value)

          const payload: KafkaPayload = {
            key,
            data: parsedValue,
          }

          subscribers.forEach(([_, fn]) => fn(payload))
        } catch (e) {
          console.error(e)
        }
      },
    })
  }

  subscribe(id: string, key: string, fn: (payload: KafkaPayload) => unknown) {
    if (this.subscriptions.has(key)) {
      this.subscriptions.set(key, [...this.subscriptions.get(key)!, [id, fn]])
    } else {
      this.subscriptions.set(key, [[id, fn]])
    }
  }

  unsubscribe(id: string, key: string) {
    if (this.subscriptions.has(key)) {
      this.subscriptions.set(
        key,
        this.subscriptions.get(key)!.filter(([sId]) => sId !== id)
      )
    }
  }
}
