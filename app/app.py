import datetime
import boto3

import scraper
import aws_s3_manager as s3_manager


def handler(event, context):
    scr = scraper.DataScraper()
    start_date = datetime.datetime.strptime(str(event['season_dates'][0]), "%Y%m%d")
    end_date = datetime.datetime.today() - datetime.timedelta(days=1)
    # end_date = event['season_dates'][1]#today.strftime("%Y%m%d")
    numdays = (end_date - start_date).days
    date_list = [(end_date - datetime.timedelta(days=x)).strftime("%Y%m%d") for x in range(numdays)]
    date_list = date_list[::-1]

    s3_client = boto3.client('s3')
    response = s3_client.list_objects(Bucket=event['bucket'])

    scraped_dates = [content['Key'].split('/')[1][0:8] for content in response['Contents']]
    dates_to_scrape = [date for date in date_list if date not in scraped_dates]

    for date in dates_to_scrape:
        games = scr.get_games(date)
        boxscores = scr.get_boxscores(games, date)
        upload_status = s3_manager.upload_object(boxscores, event['bucket'], event['folder_path']+date+'_boxscores.csv')
        if upload_status:
            return "Successfully loaded boxscore data for {} to bucket {}. Folder path {}.".format(date,
                                                                                                   event['bucket'],
                                                                                                   event['folder_path']
                                                                                                   )
