from elasticsearch import Elasticsearch


def analyze_reviews():
    """
    Analyze the reviews data and print out the following:
    1. Total number of records
    2. Top 10 reviewers with their number of reviews
    3. Top 10 products with their number of reviews and average rating
    """
    esearch = Elasticsearch(["http://localhost:9200"])

    # 1. Total number of records
    total_count = esearch.count(index="reviews")
    print(f"\n1. Total number of records: {total_count['count']}")

    # 2. Top 10 reviewers
    reviewer_query = {
        "size": 0,
        "aggs": {"top_reviewers": {"terms": {"field": "reviewerID", "size": 10}}},
    }

    reviewer_results = esearch.options(
        ignore_status=[400, 404], request_timeout=10
    ).search(index="reviews", body=reviewer_query)
    print("\n2. Top 10 reviewers:")
    for bucket in reviewer_results["aggregations"]["top_reviewers"]["buckets"]:
        reviewer_id = bucket["key"] if bucket else "Anonimous"
        reviewer_name = get_name(reviewer_id, esearch)
        print(
            f"ReviewerId: {reviewer_id}, ReviewerName: {reviewer_name}, Reviews: {bucket['doc_count']}"
        )

    # 3. Top 10 products
    product_query = {
        "size": 0,
        "aggs": {
            "top_products": {
                "terms": {"field": "asin", "size": 10},
                "aggs": {"avg_rating": {"avg": {"field": "overall"}}},
            }
        },
    }

    product_results = esearch.options(
        ignore_status=[400, 404], request_timeout=10
    ).search(index="reviews", body=product_query)
    print("\n3. Top 10 products:")
    for bucket in product_results["aggregations"]["top_products"]["buckets"]:
        print(
            f"Product ID: {bucket['key']}, "
            f"Reviews: {bucket['doc_count']}, "
            f"Average Rating: {bucket['avg_rating']['value']:.2f}"
        )


def get_name(reviewer_id, esearch):
    """
    Return the reviewer name for a given reviewer_id.

    Parameters:
    reviewer_id (str): The ID of the reviewer.
    esearch (Elasticsearch): An Elasticsearch client object.

    Returns:
    str: The name of the reviewer.
    """
    query = {"query": {"term": {"reviewerID": reviewer_id}}}
    response = esearch.options(ignore_status=[400, 404], request_timeout=10).search(
        index="reviews", body=query
    )
    return response["hits"]["hits"][0]["_source"]["reviewerName"]


if __name__ == "__main__":
    analyze_reviews()
