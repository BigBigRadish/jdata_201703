#-*- coding: utf-8 -*-

import pandas as pd
import numpy as np

ACTION_201602_FILE = "data/JData_Action_201602.csv"
ACTION_201603_FILE = "data/JData_Action_201603.csv"
ACTION_201603_EXTRA_FILE = "data/JData_Action_201603_extra.csv"
ACTION_201604_FILE = "data/JData_Action_201604.csv"
COMMENT_FILE = "data/JData_Comment.csv"
PRODUCT_FILE = "data/JData_Product.csv"
USER_FILE = "data/JData_User.csv"
NEW_USER_FILE = "data/JData_User_New.csv"
USER_TABLE_FILE = "data/user_table.csv"


def get_from_action_data(fname, chunk_size=100000):
    reader = pd.read_csv(fname, header=0, iterator=True)
    chunks = []
    loop = True
    while loop:
        try:
            chunk = reader.get_chunk(chunk_size)[
                ["user_id", "sku_id", "type", "time"]]
            chunks.append(chunk)
        except StopIteration:
            loop = False
            print("Iteration is stopped")

    df_ac = pd.concat(chunks, ignore_index=True)
    df_ac = df_ac[df_ac['type'] == 4]

    return df_ac[["user_id", "sku_id", "time"]]


def merge_weekday_action_data():
    df_ac = []
    df_ac.append(get_from_action_data(fname=ACTION_201602_FILE))
    df_ac.append(get_from_action_data(fname=ACTION_201603_FILE))
    df_ac.append(get_from_action_data(fname=ACTION_201603_EXTRA_FILE))
    df_ac.append(get_from_action_data(fname=ACTION_201604_FILE))

    df_ac = pd.concat(df_ac, ignore_index=True)
    # data type
    print(df_ac)
    print(df_ac.dtypes)
    # Monday = 0, Sunday = 6
    df_ac['time'] = pd.to_datetime(
        df_ac['time']).apply(lambda x: x.weekday() + 1)

    df_user = df_ac.groupby('time')['user_id'].nunique()
    # df_ac = pd.DataFrame({'weekday': df_ac.index, 'user_num': df_ac.values})
    df_user = df_user.to_frame().reset_index()
    df_user.columns = ['weekday', 'user_num']
    print(df_user)

    df_item = df_ac.groupby('time')['sku_id'].nunique()
    df_item = df_item.to_frame().reset_index()
    df_item.columns = ['weekday', 'item_num']
    print(df_item)

    df_ui = df_ac.groupby('time', as_index=False).size()
    df_ui = df_ui.to_frame().reset_index()
    df_ui.columns = ['weekday', 'user_item_num']
    print(df_ui)

merge_weekday_action_data()


df_ac = get_from_action_data(fname=ACTION_201602_FILE)

df_ac['time'] = pd.to_datetime(df_ac['time']).apply(lambda x: x.day)
