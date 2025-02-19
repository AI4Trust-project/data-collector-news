import json
import os
import urllib.parse
import urllib.request
import urllib.robotparser

from kafka import KafkaProducer

USER_AGENT = "Ai4Trust"


def init_context(context):
    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get("KAFKA_BROKER")],
        key_serializer=lambda x: x.encode("utf-8"),
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    setattr(context, "producer", producer)


def robots_checker(url, user_agent="*", timeout=5):
    """Check if a URL is allowed to be scraped according to the site's robots.txt file."""
    robots_url = urllib.parse.urljoin(url, "/robots.txt")
    rp = urllib.robotparser.RobotFileParser()

    try:
        req = urllib.request.Request(
            robots_url, data=None, headers={"User-Agent": USER_AGENT}
        )
        with urllib.request.urlopen(req, timeout=timeout) as response:
            robots_txt = response.read().decode("utf-8")
            rp.parse(robots_txt.splitlines())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            # not found, implicit allow
            return True
        print(f"HTTP Error: {e.code} {e.reason}")
        return False
    except Exception as e:
        print(f"{url} | Error checking robots.txt: {e}")
        return False

    return rp.can_fetch(user_agent, url)


def check_article(fetched_article):
    url = fetched_article["url"]
    can_fetch = robots_checker(url)
    return can_fetch


def handler(context, event):
    producer = context.producer

    fetched_article = json.loads(event.body.decode("utf-8"))
    context.logger.debug("Check url " + fetched_article["url"])

    if check_article(fetched_article):
        msg_key = fetched_article["search_id"] + "|" + fetched_article["id"]

        producer.send(
            "news.approved_articles",
            key=msg_key,
            value=json.loads(json.dumps(fetched_article)),
        )
