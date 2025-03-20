import json
import os
import uuid
from datetime import datetime, timezone

from gdeltdoc import Filters, GdeltDoc
from kafka import KafkaProducer


def init_context(context):
    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get("KAFKA_BROKER")],
        key_serializer=lambda x: x.encode("utf-8"),
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    setattr(context, "producer", producer)


def gdelt_fetcher(search_parameters):
    """Fetch articles from GDELT based on search filters."""
    gd = GdeltDoc()
    f = Filters(
        keyword=search_parameters.get("keyword"),
        num_records=search_parameters.get("num_records"),
        start_date=search_parameters.get("start_date"),
        end_date=search_parameters.get("end_date"),
        country=search_parameters.get("country"),
        domain=search_parameters.get("domain"),
        domain_exact=search_parameters.get("domain_exact"),
        theme=search_parameters.get("theme"),
        near=search_parameters.get("near"),
        repeat=search_parameters.get("repeat"),
    )
    try:
        fetched_articles = gd.article_search(f)
        return fetched_articles
    except Exception as e:
        print(f"Error fetching articles: {e}")
        return None


def fetch_articles(search_parameters):
    fetched_articles = gdelt_fetcher(search_parameters)
    return fetched_articles


def handler(context, event):
    producer = context.producer

    try:
        search_parameters = json.loads(event.body.decode("utf-8"))
        search_id = search_parameters["id"]
        context.logger.info(f"Fetch articles for search {search_id}")
        fetched_articles = fetch_articles(search_parameters)
        search_results = 0

        if fetched_articles is not None:
            search_results = len(fetched_articles)

        context.logger.debug(f"Fetched articles for search: {search_results}")

        if fetched_articles is not None:
            for _, row in fetched_articles.iterrows():
                try:
                    article_message = row.to_dict()
                    article_message["search_date"] = datetime.now().strftime("%Y-%m-%d")

                    # add additional info
                    article_message["data_owner"] = search_parameters.get(
                        "data_owner", "FBK"
                    )
                    article_message["created_at"] = search_parameters.get(
                        "created_at",
                        datetime.now()
                        .astimezone(timezone.utc)
                        .strftime("%Y-%m-%dT%H:%M:%SZ"),
                    )
                    article_message["search_id"] = search_parameters.get("id", None)
                    article_message["keyword_id"] = search_parameters.get(
                        "keyword_id", None
                    )
                    article_message["keyword"] = search_parameters.get("keyword", None)
                    article_message["topic"] = search_parameters.get("topic", None)
                    article_message["id"] = str(uuid.uuid4())

                    msg_key = article_message["search_id"] + "|" + article_message["id"]

                    article_json = json.loads(json.dumps(article_message))

                    producer.send(
                        "news.fetched_articles", key=msg_key, value=article_json
                    )
                except Exception as e:
                    print(e)
                    continue

    except Exception as e:
        print(f"Error in fetching process: {e}")
