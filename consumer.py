import json
import typing
from confluent_kafka import Consumer, KafkaError


def main() -> None:
    consumer: Consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9091",
            "group.id": "counting-group",
            "client.id": "client-1",
            "enable.auto.commit": True,
            "session.timeout.ms": 6000,
            "default.topic.config": {"auto.offset.reset": "smallest"},
        }
    )

    consumer.subscribe(["light_bulb"])

    last_timestamp: int = -1
    count: typing.Dict[str, int] = {"short": 0, "long": 0}

    try:
        while True:
            msg = consumer.poll(0.1)
            if msg is None:
                continue
            elif not msg.error():
                data = json.loads(msg.value())
                if data["new_status"] == "on":
                    last_timestamp = data["timestamp"]
                elif last_timestamp != -1:
                    if data["timestamp"] - last_timestamp > 0:
                        if data["timestamp"] - last_timestamp < 0.075:
                            count["short"] += 1
                        else:
                            count["long"] += 1
                    else:
                        print(f"Invalid timestamp for data {msg.value()}")
                print(count)
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"End of partition reached {msg.topic()}/{msg.partition()}")
            else:
                print(f"Error occured: {msg.error().str()}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
