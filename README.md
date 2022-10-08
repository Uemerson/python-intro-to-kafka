# About

A simple example as intro to Kafka and Python.

# How to run application

To run application you need to have installed Docker with Docker Compose.

Then type:

```
$ docker-compose up --build
```

After you can [open Kafdrop](http://localhost:9000/)

Install dependencies using poetry, type:

```
$ poetry install
```

To consuming messages, type:

```
$ python consumer.py
```

To publishing a morse code, type:

```
$ python producer.py --key="light-1" --string="XYZ"
```

# Reference(s)

[Intro to Kafka](https://dev.to/boyu1997/intro-to-kafka-4hn2)
