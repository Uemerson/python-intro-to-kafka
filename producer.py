import argparse
import json
import time
import typing
from confluent_kafka import Producer

code: typing.Dict[str, str] = {
    "A": "*-",
    "B": "-***",
    "C": "-*-*",
    "D": "-**",
    "E": "*",
    "F": "**-*",
    "G": "--*",
    "H": "****",
    "I": "**",
    "J": "*---",
    "K": "-*-",
    "L": "*-**",
    "M": "--",
    "N": "-*",
    "O": "---",
    "P": "*--*",
    "Q": "--*-",
    "R": "*-*",
    "S": "***",
    "T": "-",
    "U": "**-",
    "V": "***-",
    "W": "*--",
    "X": "-**-",
    "Y": "-*--",
    "Z": "--**",
}


def get_json_str(timestamp: float, new_status: str) -> str:
    body: typing.Dict[str, str | float] = {
        "timestamp": timestamp,
        "new_status": new_status,
    }
    print(json.dumps(body))
    return json.dumps(body)


def send_long(producer: Producer, topic: str, key: str | bytes) -> None:
    producer.produce(topic, key=key, value=get_json_str(time.time(), "on"))
    time.sleep(0.1)
    producer.produce(topic, key=key, value=get_json_str(time.time(), "off"))
    time.sleep(0.05)


def send_short(producer: Producer, topic: str, key: str | bytes) -> None:
    producer.produce(topic, key=key, value=get_json_str(time.time(), "on"))
    time.sleep(0.05)
    producer.produce(topic, key=key, value=get_json_str(time.time(), "off"))
    time.sleep(0.05)


def send_letter(letter: str, producer: Producer, topic: str, key: str | bytes) -> None:
    for send in code[letter]:
        if send == "*":
            send_short(producer, topic, key)
        else:
            send_long(producer, topic, key)
    time.sleep(0.1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Send string via morse code light bulb."
    )
    parser.add_argument("--key", type=str, default="1", help="key")
    parser.add_argument("--topic", type=str, default="light_bulb", help="publish topic")
    parser.add_argument("--string", type=str, default="ABC", help="string data (A-Z)")

    args = parser.parse_args()
    if not args.string.isalpha():
        raise RuntimeError("Input string should only contain letter A-Z.")

    producer: Producer = Producer({"bootstrap.servers": "localhost:9091"})
    for letter in args.string.upper():
        send_letter(letter, producer, args.topic, args.key)
    producer.flush(30)


if __name__ == "__main__":
    main()
