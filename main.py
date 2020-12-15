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

short_interval_duration = 30

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

#
# def read_facebook_api(app_id, app_secret, access_token, account_id, **kwargs):
#     return [{'a': 1, 'b': 2}, {'a': 12, 'b': 23}]


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
                'since': kwargs['since'].strftime('%Y-%m-%d'),
                'until': kwargs['until'].strftime('%Y-%m-%d')
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


def long_read_facebook_api(app_id, app_secret, access_token, account_id, **kwargs):
    iter_date = kwargs['since']
    end_date = kwargs['until']

    all_insights = []
    while iter_date + timedelta(short_interval_duration) < end_date:
        insights = read_facebook_api(app_id, app_secret, access_token, account_id, since=iter_date,
                                     until=(iter_date + timedelta(short_interval_duration)))
        all_insights += insights
        iter_date = iter_date + timedelta(short_interval_duration)
    if iter_date < end_date:
        all_insights += read_facebook_api(app_id, app_secret, access_token, account_id, since=iter_date,
                                          until=end_date)
    return all_insights

#
# def get_facebook_data(event, context):
#     # bigquery_client = bigquery.Client()
#     bigquery_client = 'BLYAT'
#
#     end_date = (date.today() - timedelta(1))
#     if 'end_date' in event['attributes']:
#         end_date = datetime.strptime(event['attributes']['end_date'], '%Y-%m-%d')
#
#     dataset_id = event['attributes']['dataset_id']
#     project_id = event['attributes']['project_id']
#     table_id = event['attributes']['table_id']
#
#     app_id = event['attributes']['app_id']
#     app_secret = event['attributes']['app_secret']
#     access_token = event['attributes']['access_token']
#     account_id = event['attributes']['account_id']
#
#     start_date = datetime.strptime(event['attributes']['start_date'], '%Y-%m-%d')
#
#     fill_up = False
#     if 'fill_up' in event['attributes']:
#         fill_up = True
#
#
#     TIMEEEE = (end_date - end_date).days
#     if (end_date - start_date).days > short_interval_duration:
#         long_term_table_id = table_id + '_long'
#         long_term_table_end_date = end_date - timedelta(short_interval_duration)
#         if fill_up:
#             # FILL UP ALL THE TABLE
#             # check_or_create_dataset(bigquery_client, project_id, dataset_id)
#             # delete_existing_table(bigquery_client, project_id, dataset_id, long_term_table_id)
#             # table_obj = create_table(bigquery_client, project_id, dataset_id, long_term_table_id, schema_facebook_stat,
#             #                          clustering_fields_facebook)
#             insights = long_read_facebook_api(app_id, app_secret, access_token, account_id, since=start_date,
#                                               until=long_term_table_end_date)
#             writable_insights = transform_insights(insights)
#             # if table_obj:
#             #     insert_rows_bq_obj(bigquery_client, table_obj, writable_insights)
#             # else:
#             #     insert_rows_bq(bigquery_client, long_term_table_id, dataset_id, project_id, writable_insights)
#         else:
#             # INSERT ONLY ONE DAY INSTEAD
#             last_day_date = end_date - timedelta(short_interval_duration)
#             check_or_create_dataset(bigquery_client, project_id, dataset_id)
#             insights = read_facebook_api(app_id, app_secret, access_token, account_id, since=last_day_date,
#                                          until=last_day_date)
#             writable_insights = transform_insights(insights)
#             insert_rows_bq(bigquery_client, long_term_table_id, dataset_id, project_id, writable_insights)
#         # after everything done here, we can rewrite start_date
#         start_date = end_date - timedelta(short_interval_duration)
#     short_term_table_id = table_id + '_short'
#     # Fast google BQ api operations first!!!
#     check_or_create_dataset(bigquery_client, project_id, dataset_id)
#     delete_existing_table(bigquery_client, project_id, dataset_id, short_term_table_id)
#     table_obj = create_table(bigquery_client, project_id, dataset_id, short_term_table_id, schema_facebook_stat,
#                              clustering_fields_facebook)
#     # FB API operations
#     insights = read_facebook_api(app_id, app_secret, access_token, account_id, since=start_date, until=end_date)
#
#     writable_insights = transform_insights(insights)
#     if table_obj:
#         insert_rows_bq_obj(bigquery_client, table_obj, writable_insights)
#     else:
#         insert_rows_bq(bigquery_client, short_term_table_id, dataset_id, project_id, writable_insights)
#     return 'ok'



def get_facebook_data(event, context):
    bigquery_client = bigquery.Client()

    end_date = (date.today() - timedelta(1))
    if 'end_date' in event['attributes']:
        end_date = datetime.strptime(event['attributes']['end_date'], '%Y-%m-%d')

    dataset_id = event['attributes']['dataset_id']
    project_id = event['attributes']['project_id']
    table_id = event['attributes']['table_id']

    app_id = event['attributes']['app_id']
    app_secret = event['attributes']['app_secret']
    access_token = event['attributes']['access_token']
    account_id = event['attributes']['account_id']

    start_date = datetime.strptime(event['attributes']['start_date'], '%Y-%m-%d')

    fill_up = False
    if 'fill_up' in event['attributes']:
        fill_up = True

    if (start_date - end_date).days > short_interval_duration:
        long_term_table_id = table_id + '_long'
        long_term_table_end_date = end_date-timedelta(short_interval_duration)
        if fill_up:
            # FILL UP ALL THE TABLE
            check_or_create_dataset(bigquery_client, project_id, dataset_id)
            delete_existing_table(bigquery_client, project_id, dataset_id, long_term_table_id)
            table_obj = create_table(bigquery_client, project_id, dataset_id, long_term_table_id, schema_facebook_stat,
                                     clustering_fields_facebook)
            insights = long_read_facebook_api(app_id, app_secret, access_token, account_id, since=start_date,
                                         until=long_term_table_end_date)
            writable_insights = transform_insights(insights)
            if table_obj:
                insert_rows_bq_obj(bigquery_client, table_obj, writable_insights)
            else:
                insert_rows_bq(bigquery_client, long_term_table_id, dataset_id, project_id, writable_insights)
        else:
            # INSERT ONLY ONE DAY INSTEAD
            last_day_date = end_date-timedelta(short_interval_duration)
            check_or_create_dataset(bigquery_client, project_id, dataset_id)
            insights = read_facebook_api(app_id, app_secret, access_token, account_id, since=last_day_date,
                                         until=last_day_date)
            writable_insights = transform_insights(insights)
            insert_rows_bq(bigquery_client, long_term_table_id, dataset_id, project_id, writable_insights)
        # after everything done here, we can rewrite start_date
        start_date = end_date-timedelta(short_interval_duration)
    short_term_table_id = table_id+'_short'
    # Fast google BQ api operations first!!!
    check_or_create_dataset(bigquery_client, project_id, dataset_id)
    delete_existing_table(bigquery_client, project_id, dataset_id, short_term_table_id)
    table_obj = create_table(bigquery_client, project_id, dataset_id, short_term_table_id, schema_facebook_stat,
                             clustering_fields_facebook)
    # FB API operations
    insights = read_facebook_api(app_id, app_secret, access_token, account_id, since=start_date, until=end_date)

    writable_insights = transform_insights(insights)
    if table_obj:
        insert_rows_bq_obj(bigquery_client, table_obj, writable_insights)
    else:
        insert_rows_bq(bigquery_client, short_term_table_id, dataset_id, project_id, writable_insights)
    return 'ok'

if __name__ == '__main__':
    event = {
        'attributes': {
            'end_date': '2020-12-12',
            'start_date': '2020-06-12',
            'table_id': 'huinya',
            'dataset_id': 'blyat',
            'project_id': 'sdgfsa',
            'app_id': '123142',
            'app_secret': '81874',
            'access_token': '123141',
            'account_id': '1',
            'fill_up': ''
        }
    }
    get_facebook_data(event, None)
