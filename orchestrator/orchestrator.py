import json
import os
import uuid
from datetime import datetime, timedelta, timezone

import psycopg2
from kafka import KafkaProducer
import time

DBNAME = os.environ.get("DATABASE_NAME")
USER = os.environ.get("DATABASE_USER")
PASSWORD = os.environ.get("DATABASE_PWD")
HOST = os.environ.get("DATABASE_HOST")
DELAY = 15


def init_context(context):
    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get("KAFKA_BROKER")],
        key_serializer=lambda x: x.encode("utf-8"),
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    setattr(context, "producer", producer)


def get_keywords(conn, kid):
    """Get keywords from database"""
    cur = None
    data = []

    try:

        cur = conn.cursor()

        query = "SELECT keyword_id, keyword, num_records, country, data_owner, domain, domain_exact, theme, near, repeat_ FROM news.search_keywords ORDER BY keyword_id"
        if kid and kid > 0:
            query = f"SELECT keyword_id, keyword, num_records, country, data_owner, domain, domain_exact, theme, near, repeat_ FROM news.search_keywords WHERE keyword_id='{kid}'"

        cur.execute(query)
        row = cur.fetchall()

        if row:
            for (
                keyword_id,
                keyword,
                num_records,
                country,
                data_owner,
                domain,
                domain_exact,
                theme,
                near,
                repeat_,
            ) in row:
                config = {
                    "keyword_id": keyword_id,
                    "keyword": keyword,
                    "num_records": num_records,
                    "country": country,
                    "data_owner": data_owner,
                    "domain": domain,
                    "domain_exact": domain_exact,
                    "theme": theme,
                    "near": near,
                    "repeat": repeat_,
                }
                data.append(config)
    except Exception as e:
        print("ERROR FIND KEYWORDS:")
        print(e)
    finally:
        cur.close()

    return data


def handler(context, event):
    """Generate Kafka messages based on the configuration and send them."""
    producer = context.producer

    body = event.body.decode("utf-8")
    conn = psycopg2.connect(dbname=DBNAME, user=USER, password=PASSWORD, host=HOST)

    end_date = datetime.now()
    start_date = None
    kid = None

    if body:
        parameters = json.loads(body)
        if "end_date" in parameters:
            end_date = datetime.strptime(parameters["end_date"], "%Y-%m-%d")
        if "start_date" in parameters:
            start_date = datetime.strptime(parameters["start_date"], "%Y-%m-%d")
        if "keyword_id" in parameters:
            kid = int(parameters["keyword_id"])

    if not start_date:
        start_date = end_date - timedelta(days=1)  # 1 day ago

    context.logger.info(f"Run searches for {start_date} -> {end_date}")

    configs = get_keywords(conn, kid)

    for config in configs:
        keyword_id = config.get("keyword_id", None)
        keyword = config.get("keyword", None)
        num_records = config.get("num_records", 250)
        country = config.get("country", None)
        data_owner = config.get("data_owner", None)
        # optional
        domain = config.get("domain", None)
        domain_exact = config.get("domain_exact", None)
        theme = config.get("theme", None)
        near = config.get("near", None)
        repeat = config.get("repeat", None)

        # start_date = datetime.now() - timedelta(days=365 * 5) # 5 years ago

        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        message = {
            "data_owner": data_owner,
            "created_at": datetime.now()
            .astimezone(timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%SZ"),
            "id": str(uuid.uuid4()),
            "keyword_id": keyword_id,
            "keyword": keyword,
            "num_records": num_records,
            "start_date": start_date_str,
            "end_date": end_date_str,
            "country": country,
            "domain": domain,
            "domain_exact": domain_exact,
            "theme": theme,
            "near": near,
            "repeat": repeat,
        }

        msg_key = message["id"]

        context.logger.debug(f"Run search for {country}:{keyword}")
        producer.send(
            "news.search_parameters", key=msg_key, value=json.loads(json.dumps(message))
        )

        # wait to stagger requests
        time.sleep(DELAY)

    # close connection
    conn.close()

    return context.Response(
        body=f"Run searches for {start_date} -> {end_date}",
        headers={},
        content_type="text/plain",
        status_code=200,
    )
