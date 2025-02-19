import hashlib
import json
import os
from datetime import datetime, timezone
from io import BytesIO
import uuid

import nltk
import requests
from kafka import KafkaProducer
from newspaper import Article
from PIL import Image


def init_context(context):

    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get("KAFKA_BROKER")],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    setattr(context, "producer", producer)

    # nltk.download("punkt_tab")


def hash_image(image_url):

    # Fetch the image from the image URL
    response = requests.get(image_url)
    if response.status_code == 200:
        # Open the image using PIL
        img = Image.open(BytesIO(response.content))

        # Convert image to bytes
        img_byte_arr = BytesIO()
        img.save(img_byte_arr, format=img.format)
        img_bytes = img_byte_arr.getvalue()

        # Compute MD5 hash of the image content
        img_hash = hashlib.md5(img_bytes).hexdigest()

        return img_hash


def newspaper3k_scraper(url):
    """Scrape article data from a given URL using the newspaper3k library."""
    url = url.strip()
    article = Article(url)
    try:
        article.download()
        article.parse()
        # article.nlp()
        return {
            "title": article.title,
            "authors": article.authors,
            "publish_date": (
                article.publish_date.isoformat() if article.publish_date else None
            ),
            "text": article.text,
            "summary": article.summary,
            "keywords": article.keywords,
            "source_url": article.source_url,
            "image_url": article.top_image,
        }
    except Exception as e:
        print(f"Error scraping article: {e}")
        return None


def scrape_article(approved_article):
    url = approved_article["url"]
    scraped_articled = newspaper3k_scraper(url)
    return scraped_articled


def handler(context, event):
    # Initialize Kafka Producer and Consumer
    producer = context.producer

    approved_article = json.loads(event.body.decode("utf-8"))

    scraped_article = approved_article | scrape_article(approved_article)

    if scraped_article:

        img_hash = hash_image(scraped_article["image_url"])
        scraped_article["image_hash"] = "md5:" + str(img_hash)

        # add additional info
        scraped_article["id"] = str(uuid.uuid4())
        scraped_article["fetched_id"] = approved_article.get("id", "None")
        scraped_article["data_owner"] = approved_article.get("data_owner", "FBK-NEWS")
        scraped_article["created_at"] = approved_article.get(
            "created_at",
            datetime.now().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        scraped_article["search_id"] = approved_article.get("search_id", "None")
        scraped_article["keyword_id"] = approved_article.get("keyword_id", "None")
        scraped_article["keyword"] = approved_article.get("keyword", "None")

        producer.send("news.collected_news", value=json.loads(json.dumps(scraped_article)))
