import solr
import csv


def get_doc_ids():
    ids = []
    s = solr.SolrConnection('http://localhost:8983/solr/jobs')
    response = s.query(q="*:*", fl="id", rows=10000)

    for hit in response.results:
        ids.append(str(hit['id']))

    return ids


def parse_features():
    query = []

    for feature in open("feature_list.csv"):
        query.append("termfreq(description,\"%s\")" % feature.strip())

    return query


def add_features_to_docs():
    s = solr.SolrConnection('http://localhost:8983/solr/jobs')
    features = parse_features()
    dataset = csv.writer(open("dataset.csv", "w"))

    for _id in get_doc_ids():

        response = s.query("id:%s" % _id, fields=features, score=False)
        data = response.results[0]

        row = []
        row.append(_id)

        for key in data.keys():
            row.append(data[key])

        dataset.writerow(row)


print add_features_to_docs()