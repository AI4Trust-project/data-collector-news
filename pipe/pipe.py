import json
import os

import urllib.parse
import urllib.request

import datetime
from datetime import datetime as dt

from kafka import KafkaProducer

API_KEY = os.environ.get("API_KEY")
API_URL = os.environ.get("API_URL")

LANGUAGE_CODES = {
    "english": "EN",
    "french": "FR",
    "spanish": "ES",
    "german": "DE",
    "greek": "EL",
    "italian": "IT",
    "polish": "PL",
    "romanian": "RO",
}


def init_context(context):

    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get("KAFKA_BROKER")],
        key_serializer=lambda x: x.encode("utf-8"),
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )
    setattr(context, "producer", producer)


def filter(article):
    # filter by supported languages
    return article.get("language", "None").lower() in LANGUAGE_CODES


def handler(context, event):

    article = json.loads(event.body.decode("utf-8"))

    if not filter(article):
        # skipped
        return context.Response(
            body=f"Skipped due to filtering",
            headers={},
            content_type="text/plain",
            status_code=200,
        )

    # rebuild id as composite
    id = article["search_id"] + "|" + article["id"]

    # filter content
    keys = [
        "data_owner",
        "url",
        "title",
        "text",
        "image_url",
        "keyword",
        "keyword_id",
        "topic",
    ]

    # derive language
    language = LANGUAGE_CODES.get(
        article.get("language", "None").lower(), article.get("language", None)
    )

    # parse and convert publish time
    publish_time = article.get("publish_date", None)
    if publish_time:
        try:
            publish_time = (
                dt.fromisoformat(publish_time)
                .astimezone(datetime.timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            )
        except Exception as e:
            context.logger.error(f"Error parsing publish time: {e}")
            publish_time = None

    message = (
        {"id": id}
        | {k: article.get(k) for k in keys}
        | {"language": language}
        | {"publish_time": publish_time}
    )

    context.logger.debug("Send article url " + article["url"])

    data = json.dumps(message)
    req = urllib.request.Request(API_URL, data=data.encode("utf-8"))
    req.add_header("Content-Type", "application/json")
    req.add_header("X-API-KEY", API_KEY)
    req.get_method = lambda: "POST"

    try:
        with urllib.request.urlopen(req) as f:
            response = f.read().decode("utf-8")

            # send to kafka
            value = json.loads(data) | {
                "news_id": article["id"],
                "search_id": article["search_id"],
            }
            context.producer.send(
                "pipe.news",
                key=id,
                value=value,
            )

            return context.Response(
                body=f"Response from api {response}",
                headers={},
                content_type="text/plain",
                status_code=200,
            )
    except Exception as e:
        context.logger.error(f"Error calling API: {e}")

        return context.Response(
            body=f"Error from api {e}",
            headers={},
            content_type="text/plain",
            status_code=500,
        )
