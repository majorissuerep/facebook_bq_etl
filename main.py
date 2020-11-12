from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import datetime, date, timedelta
import requests
import logging
import json
import base64
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adaccountuser import AdAccountUser
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.campaign import Campaign

logger = logging.getLogger()

schema_exchange_rate = [
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("currencies", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("rate", "FLOAT", mode="REQUIRED")
]

schema_facebook_stat = [
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("ad_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ad_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("adset_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("adset_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("campaign_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("campaign_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("clicks", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("impressions", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("spend", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField('conversions', 'RECORD', mode='REPEATED',
                         fields=(bigquery.SchemaField('action_type', 'STRING'),
                                 bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField('actions', 'RECORD', mode='REPEATED',
                         fields=(bigquery.SchemaField('action_type', 'STRING'),
                                 bigquery.SchemaField('value', 'STRING')))

]

clustering_fields_facebook = ['campaign_id', 'campaign_name']


def check_or_create_dataset(client, project_id, dataset_id):
    try:
        dataset_ref = "{}.{}".format(project_id, dataset_id)
        client.get_dataset(dataset_ref)  # Make an API request.
        logger.info("Existing dataset've been found {}".format(dataset_ref))
    except NotFound:
        dataset_ref = "{}.{}".format(project_id, dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        dataset = client.create_dataset(dataset)  # Make an API request.
        logger.info("Created dataset {}.{}".format(client.project, dataset.dataset_id))


def delete_existing_table(client, project_id, dataset_id, table_id):
    table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)
    try:
        # DELETE TABLE IF EXISTS
        client.delete_table(table_ref, not_found_ok=True)
        logger.info("Reset table state {}".format(table_ref))
    except:
        logger.error("Resetting table went wrong {}".format(table_ref))
        raise


def create_table(client, project_id, dataset_id, table_id, schema, clustering_fields=None):
    table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)
    try:
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date"
        )
        if clustering_fields is not None:
            table.clustering_fields = clustering_fields
        table = client.create_table(table)  # Make an API request.
        logger.info("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
        return table
    except:
        logger.error("Creating new table went wrong {}".format(table_ref))
        raise


def read_facebook_api(app_id, app_secret, access_token, account_id, **kwargs):
    try:
        FacebookAdsApi.init(app_id, app_secret, access_token)
        account = AdAccount('act_' + str(account_id))
        insights = account.get_insights(fields=[
            AdsInsights.Field.account_id,
            AdsInsights.Field.campaign_id,
            AdsInsights.Field.campaign_name,
            AdsInsights.Field.adset_name,
            AdsInsights.Field.adset_id,
            AdsInsights.Field.ad_name,
            AdsInsights.Field.ad_id,
            AdsInsights.Field.spend,
            AdsInsights.Field.impressions,
            AdsInsights.Field.clicks,
            AdsInsights.Field.actions,
            AdsInsights.Field.conversions
        ], params={
            'level': 'ad',
            'time_range': {
                'since': kwargs['since'],
                'until': kwargs['until']
            }, 'time_increment': 1
        })
        logger.info('FB ADS API returned {} insights.'.format(len(insights)))
        return insights
    except Exception as e:
        logger.info(e)
        raise


def transform_insights(insights):
    fb_source = []
    for index, item in enumerate(insights):
        actions = []
        conversions = []
        if 'actions' in item:
            for i, value in enumerate(item['actions']):
                actions.append({'action_type': value['action_type'], 'value': value['value']})
        if 'conversions' in item:
            for i, value in enumerate(item['conversions']):
                conversions.append({'action_type': value['action_type'], 'value': value['value']})

        fb_source.append({'date': item['date_start'],
                          'ad_id': item['ad_id'],
                          'ad_name': item['ad_name'],
                          'adset_id': item['adset_id'],
                          'adset_name': item['adset_name'],
                          'campaign_id': item['campaign_id'],
                          'campaign_name': item['campaign_name'],
                          'clicks': item['clicks'],
                          'impressions': item['impressions'],
                          'spend': item['spend'],
                          'conversions': conversions,
                          'actions': actions
                          })
    return fb_source


def insert_rows_bq(client, table_id, dataset_id, project_id, data):
    try:
        table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)
        table_obj = client.get_table(table_ref)
        resp = client.insert_rows_json(
            json_rows=data,
            table=table_obj,
        )
        logger.info("Success uploaded {} rows to table {}".format(len(data), table_obj.table_id))
    except NotFound:
        logger.exception("CANT FIND TABLE BY STR!")
        raise


def insert_rows_bq_obj(client, table_obj, data):
    resp = client.insert_rows_json(
        json_rows=data,
        table=table_obj,
    )
    logger.info("Success uploaded {} rows to returned earlier table {}".format(len(data), table_obj.table_id))


def get_facebook_data(event, context):
    bigquery_client = bigquery.Client()
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')

    yesterday = (date.today() - timedelta(1)).strftime('%Y-%m-%d')
    if 'date' in event['attributes']:
        yesterday = event['attributes']['date'].strftime('%Y-%m-%d')

    if pubsub_message == 'get_facebook':

        table_id = event['attributes']['table_id']
        dataset_id = event['attributes']['dataset_id']
        project_id = event['attributes']['project_id']

        app_id = event['attributes']['app_id']
        app_secret = event['attributes']['app_secret']
        access_token = event['attributes']['access_token']
        account_id = event['attributes']['account_id']

        start_date = event['attributes']['start_date']
        # Fast google BQ api operations first!!!
        check_or_create_dataset(bigquery_client, project_id, dataset_id)
        delete_existing_table(bigquery_client, project_id, dataset_id, table_id)
        table_obj = create_table(bigquery_client, project_id, dataset_id, table_id, schema_facebook_stat,
                                 clustering_fields_facebook)
        # FB API operations
        insights = read_facebook_api(app_id, app_secret, access_token, account_id, since=start_date, until=yesterday)

        writable_insights = transform_insights(insights)
        if table_obj:
            insert_rows_bq_obj(bigquery_client, table_obj, writable_insights)
        else:
            insert_rows_bq(bigquery_client, table_id, dataset_id, project_id, writable_insights)
        return 'ok'