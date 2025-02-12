from elasticsearch import Elasticsearch, helpers
import json


def connect_elasticsearch():
    """Create and testing Elasticsearch connection"""
    esearch = Elasticsearch(["http://localhost:9200"])
    # Check connection
    if esearch.ping():
        print("Connected to Elasticsearch")
    else:
        print("Connection failed")
    return esearch


def create_index(esearch):
    """Create index for reviews. index represent the table"""
    index_name = "reviews"

    # Define index settings and mappings
    settings = {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "reviewerID": {"type": "keyword"},
                "asin": {"type": "keyword"},
                "reviewerName": {"type": "text"},
                "helpful": {"type": "integer"},
                "reviewText": {"type": "text"},
                "overall": {"type": "float"},
                "summary": {"type": "text"},
                "unixReviewTime": {"type": "date", "format": "epoch_second"},
                "reviewTime": {"type": "text"},
            }
        },
    }

    esearch.options(ignore_status=[400]).indices.create(index=index_name, body=settings)


def insert_data(esearch):
    """Insert data into Elasticsearch"""
    index_name = "reviews"

    # Load data from NDJSON file
    with open("review.json", "r", encoding="utf-8") as f:
        # The given file is NDJSON
        data = [json.loads(line) for line in f]

    def generate_bulk_data():
        """
        A generator that yields data in the bulk API format.

        Yields:
            dict: A dictionary containing the index name, document ID, and the document source.
        """
        for idx, record in enumerate(data, start=1):
            # using generator for better performance
            yield {"_index": index_name, "_id": idx, "_source": record}

    helpers.bulk(esearch, generate_bulk_data())
    print(f"{len(data)} records inserted.")


def delete_index(esearch):
    esearch.options(ignore_status=[400]).indices.delete(index="reviews")
    print("Index deleted succesfully")


def main():
    esearch = connect_elasticsearch()
    if not esearch.indices.exists(index="reviews"):
        create_index(esearch)
    insert_data(esearch)
    # delete_index(esearch)


if __name__ == "__main__":
    main()
