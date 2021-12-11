import os
import re
import time

import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup


class DataScraper():
    # Scraping Historical Game Data from Basketball-Reference.com
    def get_games(self, date):
        url_boxscore = "https://www.basketball-reference.com/boxscores/?month={month}&day={day}&year={year}"

        url_summaries = url_boxscore.format(month=date[4:6], day=date[6:8], year=date[0:4])
        soup_summaries = BeautifulSoup(urlopen(url_summaries), 'lxml')
        time.sleep(5)
        games = soup_summaries.find_all('div', class_='game_summary expanded nohover')

        return games

    def get_boxscores(self, games, date):
        url_parent = "https://www.basketball-reference.com"

        # print(date)
        games_df_list = []
        for game in games:
            summary = {}

            host = game.find_all('table')[1].find_all('a')[1]['href'][7:10]

            # get winner and loser
            winner = game.find('tr', class_='winner').find_all('td')
            loser = game.find('tr', class_='loser').find_all('td')

            # get game scores
            summary['winner'] = [winner[0].find('a')['href'][7:10], int(winner[1].get_text())]
            summary['loser'] = [loser[0].find('a')['href'][7:10], int(loser[1].get_text())]

            url_game = url_parent + game.find('a', text='Box Score')['href']
            soup_game = BeautifulSoup(urlopen(url_game), 'lxml')
            time.sleep(5)

            tables = soup_game.find_all('table')[2:]

            columns_basic = [th.get_text() for th in tables[0].find('thead').find_all('tr')[1].find_all('th')][1:]
            # columns_advanced = [th.get_text() for th in tables[6].find('thead').find_all('tr')[1].find_all('th')][2:]
            columns_advanced = ['TS%', 'eFG%', '3PAr', 'FTr', 'ORB%', 'DRB%', 'TRB%', 'AST%', 'STL%', 'BLK%',
                                'TOV%', 'USG%', 'ORtg', 'DRtg', 'BPM']

            game_columns = ['Name', 'Date', 'Team', 'Home', 'W', 'W_PTS', 'L', 'L_PTS']
            column_headers = game_columns + columns_basic + columns_advanced

            teams = ['winner', 'loser']
            basic_stat_template = 'box-{team}-game-basic'
            advanced_stat_template = 'box-{team}-game-advanced'

            teams_df_list = []
            for team in teams:

                if summary[team][0] == host:
                    home = 1
                else:
                    home = 0

                basic_stat = basic_stat_template.format(team=summary[team][0])
                advanced_stat = advanced_stat_template.format(team=summary[team][0])

                game_data = [date, summary[team][0], home, summary['winner'][0],
                             summary['winner'][1], summary['loser'][0], summary['loser'][1]]

                data_basic = soup_game.find('table', id=basic_stat).find('tbody').find_all('tr', class_=None)
                data_advanced = soup_game.find('table', id=advanced_stat).find('tbody').find_all('tr', class_=None)

                n = len(data_basic)

                player_names = [data_basic[i].find('a').get_text() for i in range(n)]

                player_data = []
                injury_keywords = ['Did Not Dress', 'Not With Team']

                for i in range(n):
                    if data_basic[i].find('td').get_text() not in injury_keywords:
                        data = [player_names[i]] + game_data + \
                               [td.get_text() for td in data_basic[i].find_all('td')] + \
                               [td.get_text() for td in data_advanced[i].find_all('td')[1:]]

                        player_data.append(data)

                df = pd.DataFrame(player_data, columns=column_headers)
                df.columns = df.columns.str.replace('%', '_perc').str.replace('/', '')
                df = df.fillna(0)
                df.loc[:, 'FG':'+-'] = df.loc[:, 'FG':'+-'].apply(pd.to_numeric)
                df['MP'] = [0.00 if ':' not in t else round(int(t.split(':')[0]) + int(t.split(':')[1]) / 60, 2) for
                            t in df['MP']]
                teams_df_list.append(df)
                # df.to_csv(os.path.join(*[data_dir, 'Boxscores', season, date + '-' + summary[team][0] + '.csv']),
                #           index=False)
            game_df = pd.concat(teams_df_list)
            games_df_list.append(game_df)

            time.sleep(1)
        boxscores_df = pd.concat(games_df_list)

        return boxscores_df

    # Scraping DraftKings salary data from RotoGuru.com
    def get_fantasy_salary(self, season, date_list, data_dir):
        url_roto = "http://rotoguru1.com/cgi-bin/hyday.pl?mon={month}&day={day}&year={year}&game=dk"
        print("Scraping salary information from the {} regular season".format(season))

        for date in date_list:
            teams, positions, players, starters, salaries = [], [], [], [], []

            url_date = url_roto.format(month=date[4:6], day=date[6:8], year=date[0:4])
            soup = BeautifulSoup(urlopen(url_date), 'lxml')

            # Check if there were any games on a given date
            soup_table = soup.find('body').find('table', border="0", cellspacing="5")

            soup_rows = soup_table.find_all('tr')

            for row in soup_rows:
                if row.find('td').has_attr('colspan') == False:
                    if row.find('a').get_text() != '':

                        position = row.find_all('td')[0].get_text()

                        player_tmp = row.find('a').get_text().split(", ")
                        player = player_tmp[1] + ' ' + player_tmp[0]

                        starter_tmp = row.find_all('td')[1].get_text()
                        if '^' in starter_tmp:
                            starter = 1
                        else:
                            starter = 0

                        salary_tmp = row.find_all('td')[3].get_text()
                        salary = re.sub('[$,]', '', salary_tmp)

                        team = row.find_all('td')[4].get_text()

                        positions.append(position)
                        players.append(player)
                        starters.append(starter)
                        salaries.append(salary)
                        teams.append(team)

            df = pd.DataFrame({'Date': [date for i in range(len(players))],
                               'Team': [team.upper() for team in teams],
                               'Starter': starters,
                               'Pos': positions,
                               'Name': players,
                               'Salary': salaries})

            df = df.loc[:, ['Date', 'Team', 'Pos', 'Name', 'Starter', 'Salary']]

            df.to_csv(os.path.join(data_dir, 'DKSalary', season, 'salary_' + date + '.csv'), index=False)

        time.sleep(1)
        return None

# start_date = '20191022'
# end_date = '20200311'
# season = '2019-20'
# date_list = [d.strftime('%Y%m%d') for d in pd.date_range(start_date,end_date)]
# data_dir = '/Users/admin/Documents/Data Science/Project Answer/dev/data/'
# # season_dates = {
# #     '2014-15': ['20141028', '20150415'],
# #     '2015-16': ['20151027', '20160413'],
# #     '2016-17': ['20161025', '20170412'],
# #     '2017-18': ['20171017', '20180411'],
# #     '2018-19': ['20181016', '20190410'],
# #     '2019-20': ['20191022', '20200410']
# # }
#
# season_dates = {
#     # '2018-19': ['20190201', '20190410'],
#     # '2020-21': ['20201222', '20210406'],
#     '2021-22': ['20211019', '20211020']
# }
#
# scraper = DataScraper()

# Comment out season dates in SEASON_DATES in constants.py to extract data for specific seasons
# for data_type in ['Boxscores', 'DKSalary']:
#     for season in season_dates.keys():
#         if not os.path.exists(os.path.join(data_dir, data_type, season)):
#             # Create a new directory and scrape the entire season
#             os.mkdir(os.path.join(data_dir, data_type, season))
#             start_date = season_dates[season][0]
#             end_date = season_dates[season][1]
#             date_list = [d.strftime('%Y%m%d') for d in pd.date_range(start_date, end_date)]
#
#             if data_type == 'Boxscores':
#                 scraper.get_boxscores(season, date_list, data_dir)
#             else:
#                 scraper.get_fantasy_salary(season, date_list, data_dir)
#
#         elif os.path.exists(os.path.join(data_dir, data_type, season)):
#             # Iterate over the existing files by name and scrape missing dates
#             start_date = season_dates[season][0]
#             end_date = season_dates[season][1]
#             # Dates to scrape box scores from
#             date_list = [d.strftime('%Y%m%d') for d in pd.date_range(start_date, end_date)]
#
#             if data_type == 'Boxscores':
#                 for date in date_list:
#                     # Check if csv files of the form {date}-{hometeam}.csv (i.e. 20131029-CHI.csv) exists
#                     if len(glob.glob(os.path.join(data_dir, data_type, season, str(date) + "*.csv"))) > 0:
#                         # Set back the start day by
#                         date_list = date_list[date_list.index(date):]
#
#                 scraper.get_boxscores(season, date_list, data_dir)
#
#             else:
#                 for date in date_list:
#                     # Check if csv files of the form salary_{date}.csv (i.e. salary_20131029.csv) exists
#                     if os.path.exists(os.path.join(data_dir, data_type, season, "salary_{}.csv".format(date))):
#                         date_list = date_list[date_list.index(date):]
#
#                 scraper.get_fantasy_salary(season, date_list, data_dir)


