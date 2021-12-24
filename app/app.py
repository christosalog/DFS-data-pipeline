import datetime
import boto3
import json

import scraper
import aws_s3_manager as s3_manager


def handler(event_dict, context):
    scr = scraper.DataScraper()
    start_date = datetime.datetime.strptime(str(event_dict['season_dates'][0]), "%Y%m%d")
    end_date = datetime.datetime.today() - datetime.timedelta(days=1)
    numdays = (end_date - start_date).days
    date_list = [(end_date - datetime.timedelta(days=x)).strftime("%Y%m%d") for x in range(numdays)]
    date_list = date_list[::-1]
    no_game_dates = ['20211125', '20211224']
    date_list = [date for date in date_list if date not in no_game_dates]

    s3_client = boto3.client('s3')
    response = s3_client.list_objects(Bucket=event_dict['bucket'])

    if event_dict['data'] == 'boxscore':
        scraped_dates = [content['Key'].split('/')[1][0:8] for content in response['Contents'] if 'boxscores' in content['Key'].split('/')[1]]
        dates_to_scrape = [date for date in date_list if date not in scraped_dates]
        if len(dates_to_scrape) == 0:
            return 'There are no dates to scrape'

        for date in dates_to_scrape:
            games = scr.get_games(date)
            boxscores = scr.get_boxscores(games, date)
            upload_status = s3_manager.upload_object(boxscores, event_dict['bucket'], event_dict['folder_path']+date+'_boxscores.csv')
        return "Successfully loaded boxscore data for {} to bucket {}. Folder path {}.".format(date,
                                                                                               event_dict['bucket'],
                                                                                               event_dict['folder_path']
                                                                                               )

    elif event_dict['data'] == 'salary':
        scraped_dates = [content['Key'].split('/')[1][0:8] for content in response['Contents'] if
                         'salaries' in content['Key'].split('/')[1]]
        dates_to_scrape = [date for date in date_list if date not in scraped_dates]
        if len(dates_to_scrape) == 0:
            return 'There are no dates to scrape'

        for date in dates_to_scrape:
            salaries = scr.get_fantasy_salary(date)
            upload_status = s3_manager.upload_object(salaries, event_dict['bucket'],
                                                     event_dict['folder_path'] + date + '_salaries.csv')
        return "Successfully loaded salary data for {} to bucket {}. Folder path {}.".format(date,
                                                                                               event_dict['bucket'],
                                                                                               event_dict[
                                                                                                   'folder_path']
                                                                                               )

