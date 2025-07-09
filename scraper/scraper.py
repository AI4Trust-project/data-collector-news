import hashlib
import json
import os
from datetime import datetime, timezone
from io import BytesIO
import uuid
import base64

import requests
from kafka import KafkaProducer
from newspaper import Article, Config
from PIL import Image
from textwrap import wrap
import boto3
import botocore

S3_BUCKET = os.environ.get("S3_BUCKET")
USER_AGENT = "libcurl"


def init_context(context):

    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get("KAFKA_BROKER")],
        key_serializer=lambda x: x.encode("utf-8"),
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )
    setattr(context, "producer", producer)

    s3c = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=None,
        config=boto3.session.Config(signature_version="s3v4"),
        verify=False,
    )
    setattr(context, "s3c", s3c)

    # nltk.download("punkt_tab")


def key_exists(client, bucket, key):
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            raise


def fetch_image(image_url, s3c):

    # Fetch the image from the image URL
    response = requests.get(image_url)
    if response.status_code == 200:
        # Open the image using PIL
        img = Image.open(BytesIO(response.content))

        # Convert image to bytes
        img_byte_arr = BytesIO()
        img.save(img_byte_arr, format=img.format)
        img_bytes = img_byte_arr.getvalue()

        # Compute SHA hash of the image content
        img_hash = hashlib.sha256(img_bytes)
        img_sha = img_hash.hexdigest()

        # Upload
        img_path = (
            "news/artifacts/imgs/" + "/".join(wrap(img_sha[:12], 4)) + "/" + img_sha
        )

        # dedup upload
        if not key_exists(s3c, S3_BUCKET, img_path):
            # upload with checksum enabled
            s3c.put_object(
                Body=img_bytes,
                Bucket=S3_BUCKET,
                Key=img_path,
                ContentType=response.headers.get("content-type"),
                ChecksumAlgorithm="SHA256",
                ChecksumSHA256=base64.b64encode(img_hash.digest()).decode("utf-8"),
            )

        return f"s3://{S3_BUCKET}/{img_path}", f"sha256:{img_sha}"


def newspaper3k_scraper(url):
    """Scrape article data from a given URL using the newspaper3k library."""
    url = url.strip()
    config = Config()
    config.browser_user_agent = USER_AGENT
    article = Article(url, config=config)
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
            "video_url": article.movies[0] if article.movies else None,
        }
    except Exception as e:
        print(f"Error scraping article: {e}")
        return None


def handler(context, event):
    # Initialize Kafka Producer and Consumer
    producer = context.producer

    approved_article = json.loads(event.body.decode("utf-8"))

    context.logger.debug("Scrape article from " + approved_article["url"])

    scraped_article = newspaper3k_scraper(approved_article["url"])
    if scraped_article:
        # optional thumbnail
        if scraped_article["image_url"]:
            img_path, img_hash = fetch_image(scraped_article["image_url"], context.s3c)
            scraped_article["image_hash"] = img_hash
            scraped_article["image_path"] = img_path

        # optional video
        if scraped_article["video_url"]:
            video_url = scraped_article["video_url"]
            if not video_url.startswith("https://") and not video_url.startswith("http://") and scraped_article["source_url"]:
                video_url = scraped_article["source_url"] + video_url
                scraped_article["video_url"] = video_url

        keys = [
            "url",
            "title",
            "domain",
            "language",
            "sourcecountry",
            "data_owner",
            "search_id",
            "keyword_id",
            "keyword",
            "topic",
            "id",
        ]

        message = (
            {k: approved_article[k] for k in keys}
            | {
                "created_at": datetime.now()
                .astimezone(timezone.utc)
                .strftime("%Y-%m-%dT%H:%M:%SZ")
            }
            | scraped_article
        )

        msg_key = message["search_id"] + "|" + message["id"]

        producer.send(
            "news.collected_news",
            key=msg_key,
            value=json.loads(json.dumps(message)),
        )
