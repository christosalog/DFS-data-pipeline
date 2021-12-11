#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 10:06:54 2020

@author: admin
"""

'''merge all data files from 2019-20'''

import pandas as pd
from os import listdir
from src.FeatureEngineering.feature_eng_functions import *

def merge_game_bs(season):
    '''process box scores'''
    data_dir = "/Users/admin/Documents/Data Science/Project Answer/data/Games/" + season + "/"


    df_list = []
    [df_list.append(pd.read_csv(data_dir + f)) for f in listdir(data_dir)]

    df_all = pd.concat(df_list)

    # df_all.to_csv("/Users/admin/Documents/Data Science/Project Answer/data/Games/"+season+"/udacity_"+season+".csv", index=False)
    return df_all

def merge_salary_data(season):
    '''process salary data'''
    dkdata_dir = "/Users/admin/Documents/Data Science/Project Answer/data/DKSalary/"+season+"/"


    df_list = []
    [df_list.append(pd.read_csv(dkdata_dir + f)) for f in listdir(dkdata_dir)]

    df_all = pd.concat(df_list)

    # df_all.to_csv("/Users/admin/Documents/Data Science/Project Answer/data/DKSalary/"+season+"/dksalary_"+season+".csv", index=False)
    return df_all

def join_boxscore_salary(season, df_bs, df_salary):
    '''merge dksalary with box score data'''
    # df_bs = pd.read_csv("/Users/admin/Documents/Data Science/Project Answer/data/Games/"+season+"/udacity_"+season+".csv")
    # df_dk = pd.read_csv("/Users/admin/Documents/Data Science/Project Answer/data/DKSalary/"+season+"/dksalary_"+season+".csv")
    df_bs = rename_players(df_bs)


    df_mrg = pd.merge(df_bs, df_salary, how='left', on=['Name','Date'])
    df_mrg.to_csv("/Users/admin/Documents/Data Science/Project Answer/data/season_"+season+"_w_fixed_names.csv", index=False)

if __name__ == 'main':
    season = '2020-21'
    df_bs = merge_game_bs(season)
    df_salary = merge_salary_data(season)

    join_boxscore_salary(season, df_bs, df_salary)