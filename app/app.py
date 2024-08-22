from flask import Flask, request, jsonify
import pandas as pd
import os
from elasticsearch import Elasticsearch, helpers

app = Flask(__name__)

# Connect to Elasticsearch
es = Elasticsearch(
    hosts=[{'host': 'localhost', 'port': 9200, 'scheme': 'http'}]
)

# Define the index name
index_name = 'excel_data'


def create_index_with_ngram():
    # Delete index if it exists
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)

    # Create index with n-gram analyzer
    es.indices.create(index=index_name, body={
        "settings": {
            "analysis": {
                "filter": {
                    "ngram_filter": {
                        "type": "ngram",
                        "min_gram": 2,
                        "max_gram": 3
                    }
                },
                "analyzer": {
                    "ngram_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "ngram_filter"]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "name": {"type": "text", "analyzer": "ngram_analyzer"},
                "birthdate": {"type": "date"},
                "birthplace": {"type": "text", "analyzer": "ngram_analyzer"},
                "notes": {"type": "text", "analyzer": "ngram_analyzer"}
            }
        }
    })
    return "Index with n-gram analyzer created successfully!"


def index_excel_data():
    folder_path = 'sample-data'
    file_path = os.path.join(folder_path, 'Example-PEP-DATA.xlsx')

    # Read Excel file into DataFrame
    df = pd.read_excel(file_path)

    # Index data into Elasticsearch
    def dataframe_to_elasticsearch(dataframe):
        records = dataframe.to_dict(orient='records')
        for record in records:
            yield {
                "_index": index_name,
                "_source": record
            }

    helpers.bulk(es, dataframe_to_elasticsearch(df))
    return "Data indexed successfully!"


@app.before_first_request
def load_data():
    create_index_with_ngram()
    index_excel_data()


@app.route('/search', methods=['POST'])
def search_data():
    data = request.json
    search_text = data.get('search_text')

    if not search_text:
        return jsonify({'error': 'search_text is required'}), 400

    # Elasticsearch query with fuzziness and n-gram
    query = {
        "query": {
            "multi_match": {
                "query": search_text,
                "fields": ["name^3", "birthplace", "notes"],
                "fuzziness": "AUTO",  # Enables fuzziness
                "analyzer": "ngram_analyzer"
            }
        }
    }

    # Perform the search
    response = es.search(index=index_name, body=query)

    # Extract and return the results
    results = [hit['_source'] for hit in response['hits']['hits']]

    if not results:
        return jsonify({'message': 'No results found'}), 404

    return jsonify(results), 200


if __name__ == '__main__':
    app.run(debug=True)
