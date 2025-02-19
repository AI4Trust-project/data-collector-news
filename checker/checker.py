import json
import os
import urllib.parse
import urllib.request
import urllib.robotparser

from kafka import KafkaProducer


def init_context(context):
    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get("KAFKA_BROKER")],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    setattr(context, "producer", producer)


def robots_checker(url, user_agent="*", timeout=5):
    """Check if a URL is allowed to be scraped according to the site's robots.txt file."""
    robots_url = urllib.parse.urljoin(url, "/robots.txt")
    rp = urllib.robotparser.RobotFileParser()

    try:
        with urllib.request.urlopen(robots_url, timeout=timeout) as response:
            robots_txt = response.read().decode("utf-8")
            rp.parse(robots_txt.splitlines())
    except Exception as e:
        print(f"Error checking robots.txt: {e}")
        return False

    return rp.can_fetch(user_agent, url)


def check_article(fetched_article):
    url = fetched_article["url"]
    can_fetch = robots_checker(url)
    return can_fetch


def handler(context, event):

    producer = context.producer

    fetched_article = json.loads(event.body.decode("utf-8"))

    if check_article(fetched_article):

        producer.send("news.approved_articles", value=json.loads(json.dumps(fetched_article)))
