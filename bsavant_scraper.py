# -*- coding: utf-8 -*-

from time import sleep
from urllib.error import HTTPError
import pandas as pd


def savant_search(season, team, home_road, csv=False, sep=';'):
    """Return detail-level Baseball Savant search results.
    Breaks pieces by team, year, and home/road for reasonable file sizes.
    Args:
        season (int): the year of results.
        team (str): the modern three letter team abbreviation.
        home_road (str): whether the pitching team is "Home" or "Road".
        csv (bool): whether or not a csv
        sep (str): separat
    Returns:
        a pandas dataframe of results and optionally a csv.
    Raises:
        HTTPError: if connection is unsuccessful multiple times.
    """
    # Define the number of times to retry on a connection error
    num_tries = 6
    # Define the starting backoff time to grow exponentially
    pause_time = 30

    # Generate the URL to search based on team and year
    url = ("https://baseballsavant.mlb.com/statcast_search/csv?all=true"
           "&hfPT=&hfAB=&hfBBT=&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfGT=&h"
           f"fC=&hfSea={season}%7C&hfSit=&player_type=pitcher&hfOuts=&opponent"
           "=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt=&game_date_lt="
           f"&hfInfield=&team={team}&position=&hfOutfield=&hfRO="
           f"&home_road={home_road}&hfFlag=&hfPull=&metric_1=&hfInn="
           "&min_pitches=0&min_results=0&group_by=name&sort_col=pitches"
           "&player_event_sort=pitch_number_thisgame&sort_order=desc"
           "&min_pas=0&type=details&")

    # Attempt to download the file
    # If unsuccessful retry with exponential backoff
    # If still unsuccessful raise HTTPError
    # Due to possible limit on access to this data
    for retry in range(0, num_tries):
        try:
            single_combination = pd.read_csv(url, low_memory=False)
        except HTTPError as connect_error:

            if connect_error:
                if retry == num_tries - 1:
                    raise HTTPError
                else:
                    sleep(pause_time)
                    pause_time *= 2
                    continue
            else:
                break

    # Drop duplicate and deprecated fields
    single_combination.drop(['pitcher.1', 'fielder_2.1', 'umpire', 'spin_dir',
                             'spin_rate_deprecated', 'break_angle_deprecated',
                             'break_length_deprecated', 'tfs_deprecated',
                             'tfs_zulu_deprecated'], axis=1, inplace=True)

    # Optionally save as csv for loading to another file system
    if csv:
        single_combination.to_csv(f"{team}_{season}_{home_road}_detail.csv",
                                  index=False, sep=sep)

    return single_combination if not csv else None


"""
Scrapper obatied from the repo https://github.com/alanrkessler/savantscraper

"""
