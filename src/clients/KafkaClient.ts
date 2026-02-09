import { Kafka, type Consumer } from "kafkajs"
import type { KafkaPayload } from "../types"

export class KafkaClient {
  protected readonly client: Kafka | null
  protected readonly consumer: Consumer | null

  private keys = new Set<string>()

  // { [key]: [[id, fn], ...] }
  protected subscriptions: Map<string, [string, (payload: KafkaPayload) => unknown][]> = new Map()

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

  get allKeys() {
    return [...this.keys]
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

          if (!this.keys.has(key)) this.keys.add(key)

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
