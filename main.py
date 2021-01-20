from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import datetime, date, timedelta
import logging
import time

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights

short_interval_duration = 30
read_facebook_interval = 60

logger = logging.getLogger()

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


# TODO: CHECK TABLE AND CREATE VIEV
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
        if len(data) > 0:
            resp = client.insert_rows_json(
                json_rows=data,
                table=table_obj,
            )
        logger.info("Success uploaded {} rows to table {}".format(len(data), table_obj.table_id))
    except NotFound:
        logger.exception("CANT FIND TABLE BY STR!")
        raise


def insert_rows_bq_obj(client, table_obj, data):
    if len(data) > 0:
        resp = client.insert_rows_json(
            json_rows=data,
            table=table_obj,
        )
    logger.info("Success uploaded {} rows to returned earlier table {}".format(len(data), table_obj.table_id))


def long_read_facebook_api(app_id, app_secret, access_token, account_id, **kwargs):
    iter_date = kwargs['since']
    end_date = kwargs['until']

    all_insights = []
    while iter_date + timedelta(read_facebook_interval) < end_date:
        insights = read_facebook_api(app_id, app_secret, access_token, account_id, since=iter_date,
                                     until=(iter_date + timedelta(read_facebook_interval-1)))
        all_insights += transform_insights(insights)
        iter_date = iter_date + timedelta(read_facebook_interval)
        time.sleep(10)
    if iter_date < end_date:
        all_insights += transform_insights(read_facebook_api(app_id, app_secret, access_token, account_id, since=iter_date,
                                          until=end_date))
    return all_insights


def check_table_existance(bigquery_client, project_id, dataset_id, table_id):
    table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)
    try:
        bigquery_client.get_table(table_ref)  # Make an API request.
        return True
    except NotFound:
        return False


def get_facebook_data(event, context):
    bigquery_client = bigquery.Client()

    end_date = datetime.combine(date.today() - timedelta(1), datetime.min.time())
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

    view_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)
    long_term_table_ref = view_ref + '_historical'
    short_term_table_ref = view_ref + '_{}days'.format(short_interval_duration)

    if end_date < start_date:
        temp = end_date
        end_date = start_date
        start_date = temp

    if (end_date - start_date).days > short_interval_duration:
        long_term_table_id = long_term_table_ref.split('.')[-1]
        long_term_table_end_date = end_date - timedelta(short_interval_duration)
        if not check_table_existance(bigquery_client, project_id, dataset_id, long_term_table_id):
            # FILL UP ALL THE TABLE
            check_or_create_dataset(bigquery_client, project_id, dataset_id)
            delete_existing_table(bigquery_client, project_id, dataset_id, long_term_table_id)
            table_obj = create_table(bigquery_client, project_id, dataset_id, long_term_table_id, schema_facebook_stat,
                                     clustering_fields_facebook)
            writable_insights = long_read_facebook_api(app_id, app_secret, access_token, account_id, since=start_date,
                                              until=long_term_table_end_date)
            # writable_insights = transform_insights(insights)
            if table_obj:
                insert_rows_bq_obj(bigquery_client, table_obj, writable_insights)
            else:
                insert_rows_bq(bigquery_client, long_term_table_id, dataset_id, project_id, writable_insights)
        else:
            # INSERT ONLY ONE DAY INSTEAD
            last_day_date = end_date - timedelta(short_interval_duration)
            insights = read_facebook_api(app_id, app_secret, access_token, account_id, since=last_day_date,
                                         until=last_day_date)
            writable_insights = transform_insights(insights)
            insert_rows_bq(bigquery_client, long_term_table_id, dataset_id, project_id, writable_insights)
        # after everything done here, we can rewrite start_date so we will WRITE ONLY 30 LAST DAYS
        start_date = end_date - timedelta(short_interval_duration)
    short_term_table_id = short_term_table_ref.split('.')[-1]
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

    # CREATING VIEW
    if not check_table_existance(bigquery_client, project_id, dataset_id, table_id):
        view_query = """
        SELECT * FROM `{}`
        UNION ALL
        SELECT * FROM `{}`
        """
        view = bigquery.Table(view_ref)
        view.view_query = view_query.format(long_term_table_ref, short_term_table_ref)
        view = bigquery_client.create_table(view)
        logger.info('view table created!')
