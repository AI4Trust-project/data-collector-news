import json
import os
import uuid
from datetime import datetime, timedelta, timezone

import psycopg2
from kafka import KafkaProducer


def init_context(context):
    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get("KAFKA_BROKER")],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    dbname = os.environ.get("DATABASE_NAME")
    user = os.environ.get("DATABASE_USER")
    password = os.environ.get("DATABASE_PWD")
    host = os.environ.get("DATABASE_HOST")

    conn = psycopg2.connect(
        dbname=dbname, user=user, password=password, host=host
    )

    setattr(context, "producer", producer)
    setattr(context, "conn", conn)


def get_keywords(conn):
    """Get keywords from database"""
    cur = None
    data = []

    try:

        cur = conn.cursor()

        query = "SELECT keyword_id, keyword, num_records, country, data_owner, domain, domain_exact, theme, near, repeat_ FROM news.search_keywords"

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
    configs = get_keywords(context.conn)

    for config in configs:

        keyword_id = config.get("keyword_id", None)
        keyword = config.get("keyword", None)
        num_records = config.get("num_records", 0)
        country = config.get("country", None)
        data_owner = config.get("data_owner", None)
        # optional
        domain = config.get("domain", None)
        domain_exact = config.get("domain_exact", None)
        theme = config.get("theme", None)
        near = config.get("near", None)
        repeat = config.get("repeat", None)

        # start_date = datetime.now() - timedelta(days=365 * 5) # 5 years ago

        end_date = datetime.now()
        start_date = datetime.now() - timedelta(days=2)  # 2 days ago

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

        producer.send("news.search_parameters", value=json.loads(json.dumps(message)))
