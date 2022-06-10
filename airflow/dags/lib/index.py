try:
    import os
    import sys

    import elasticsearch
    from elasticsearch import Elasticsearch
    from elasticsearch import helpers
    import pandas as pd
    from datetime import datetime

    print("All Modules Loaded ! ")
except Exception as e:
    print("Some Modules are Missing {}".format(e))


def generator1(df2):
    for c, line in enumerate(df2):
        yield {
            '_index': 'airpollution',
            'type': '_doc',
            '_id': c,
            '_source': {
                'uuid': line.get('uuid', ""),
                'code_epci': line.get('code_epci', None),
                'code_station': line.get('code_station', None),
                'data_date': line.get('data_date', None),
                'id_poll_ue': line.get('id_poll_ue', None),
                'influence': line.get('influence', ""),
                'insee_com': line.get('insee_com', None),
                'lat': line.get('lat', None),
                'lon': line.get('lon', None),
                'nom_com': line.get('nom_com', ""),
                'nom_dept': line.get('nom_dept', ""),
                'nom_poll': line.get('nom_poll', ""),
                'nom_station': line.get('nom_station', ""),
                'statut_valid': line.get('statut_valid', None),
                'typologie': line.get('typologie', ""),
                'unite': line.get('unite', ""),
                'valeur': line.get('valeur', None)}
        }


def generator2(df2):
    for c, line in enumerate(df2):
        yield {
            '_index': 'open',
            'type': '_doc',
            '_id': c,
            '_source': {
                'lon': line.get('lon', None),
                'lat': line.get('lat', None),
                'date': datetime.utcfromtimestamp(line.get('dt', None)).strftime('%Y-%m-%d %H:%M:%S'),
                'co': line.get('co', None),
                'no': line.get('no', None),
                'no2': line.get('no2', None),
                'o3': line.get('o3', None),
                'so2': line.get('so2', None),
                'pm2_5': line.get('pm2_5', None),
                'pm10': line.get('pm10', None),
                'nh3': line.get('nh3', None)
            }
        }


def connect_elasticsearch():
    es = None
    ELASTIC_PASSWORD = "XkIbe7ic3wVQdXgaFqxd"
    es = Elasticsearch(
        "https://localhost:9200",
        ca_certs="~/elasticsearch-8.2.0/config/certs/http_ca.crt",
        basic_auth=("elastic", ELASTIC_PASSWORD)
    )
    if es.ping():
        print('Yupiee  Connected ')
    else:
        print('Awww it could not connect!')
    return es

def indeex(current_day):
    es = connect_elasticsearch()
    df = pd.read_parquet('~/BigData-AirPolution/DataLake/usage/analytics/' + current_day + '/res.snappy.parquet/')
    # dfop = pd.read_parquet('~/BigData-AirPolution/DataLake/usage/analytics/' + current_day + '/op/res.snappy.parquet/')

    df2 = df.to_dict('records')
    # dfop2 = dfop.to_dict('records')

    dff = generator1(df2)
    # dfff = generator2(dfop2)

    try:
        res = helpers.bulk(es, dff)
        # res1 = helpers.bulk(es, dfff)
        print('works')
    except Exception as e:
        print(e)
