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
    def get_fantasy_salary(self, date):
        url_roto = "http://rotoguru1.com/cgi-bin/hyday.pl?mon={month}&day={day}&year={year}&game=dk"

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

        time.sleep(1)
        return df
