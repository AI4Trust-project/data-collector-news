import json
import os

import urllib.parse
import urllib.request


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

    message = (
        {"id": id}
        | {k: article.get(k) for k in keys}
        | {"language": language}
        | {"publish_time": article["publish_date"]}
    )

    context.logger.debug("Send article url " + article["url"])

    req = urllib.request.Request(API_URL, data=json.dumps(message).encode("utf-8"))
    req.add_header("Content-Type", "application/json")
    req.add_header("X-API-KEY", API_KEY)
    req.get_method = lambda: "POST"

    try:
        with urllib.request.urlopen(req) as f:
            response = f.read().decode("utf-8")

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
