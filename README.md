# News Articles Collection

![alt text](news_diagram.png "Diagram of the News Collection")

## Pipeline Description

### Orchestrator
* Retrieves a list of keywords and configurations for targeted news searches.

### Fetcher
* Utilizes GDELT to search the internet for news articles matching the specified keywords and configurations.
* Stores links to identified articles as "fetched articles."

### Checker
* Examines the domain of each fetched article link.
* Checks for the presence of a robots.txt file.
* If a robots.txt file exists, determines if the website allows web scraping.

### Scraper (Conditional)
* Employs Newspaper3k to extract the plain text content from the article.
* Stores the scraped text as "news scraped."


## Instructions

Initialize the database (via sqlpad for example), then deploy the functions and the sink.
### Prerequisites

* Create the bucket in minio `s3://news`
* Create the database schema `datalake` -> `news`
* Create a project in nuclio `news`
* create a namespace in nessie via cli
  
https://projectnessie.org/nessie-0-96-0/cli/

launch the console (for example java -jar nessie*.jar)
```
Nessie> CONNECT TO http://192.168.58.2:30464/api/v2
main> create namespace news

```


### 1. Orchestrator

Access the nuclio console, create a project named `news` and import the `orchestrator/function.yaml` definition.
Configure the ENV if required (set DB credentials!), then build and deploy the function to start producing messages into `news.search_parameters` topic.


### 2. Fetcher

Access the nuclio console and under project named `news` import the `fetcher/function.yaml` definition.
Configure the ENV if required, then build and deploy the function to start producing messages into `news.fetched_articles` topic.




### 3. Checker

Access the nuclio console and under project named `news` import the `checker/function.yaml` definition.
Configure the ENV if required, then build and deploy the function to start producing messages into `news.approved_articles` topic.

### 4. Scraper

Access the nuclio console and under project named `news` import the `scraper/function.yaml` definition.
Configure the ENV if required, then build and deploy the function to start producing messages into `news.collected_news` topic.



### 5. Sink (Apache Iceberg)
https://github.com/databricks/iceberg-kafka-connect

Example sink to persist from topics matching `news.(.*)` to s3 warehouse as iceberg tables, registered in nessie.



```bash
kubectl apply -f sink/icebergsinkconfig.yaml
```


### 4. Monitor/View data

Access:
* nuclio console to check functions are running
* redpanda console to check topics are populated and registered to publisher and consumers
* nessie to check tables under `news.*` are created 
* sqlpad to query the content of the tables in iceberg

NOTE: trino and/or sqlpad keep a cache of known schemas and tables, so it may take a while to see them.
You can still query data, for example
```
select * from news.collected_news limit 1
```
