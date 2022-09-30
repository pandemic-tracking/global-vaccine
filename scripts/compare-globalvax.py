""" Script that runs through validated manual screenshots in Airtable and uploads them to S3. """

import os
import re
import sys

from argparse import ArgumentParser, RawDescriptionHelpFormatter
import dateutil.parser
from loguru import logger

import pandas as pd
from datetime import datetime

from utils import S3Backup


parser = ArgumentParser(
    description=__doc__,
    formatter_class=RawDescriptionHelpFormatter)

parser.add_argument(
    '--temp-dir',
    default='/tmp/public-cache',
    help='Local temp dir for snapshots')

# Args relating to S3 setup

parser.add_argument(
    '--s3-bucket',
    default='pandemic-tracking-collective-data',
    help='S3 bucket name')

parser.add_argument(
    '--s3-subfolder',
    default='globalvax',
    help='Name of subfolder on S3 bucket to upload files to')

parser.add_argument('--push-to-s3', dest='push_to_s3', action='store_true', default=False,
    help='Push screenshots to S3')


def get_merged_dataframe():
    # get OWID vaccination timeseries from Github
    owid_data = pd.read_csv("https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv")

    # drop non cumulative columns from OWID data
    owid_data.drop(columns=['daily_vaccinations','total_vaccinations_per_hundred','people_vaccinated_per_hundred','people_fully_vaccinated_per_hundred','total_boosters_per_hundred','daily_vaccinations_per_million','daily_people_vaccinated','daily_people_vaccinated_per_hundred','daily_vaccinations_raw'], inplace = True)

    # forward fill empty values in owid dataset
    #owid_data.fillna(method='ffill',inplace = True)
    owid_data[['total_vaccinations','people_vaccinated','total_boosters']] = owid_data.groupby('iso_code')[['total_vaccinations','people_vaccinated','total_boosters']].apply(lambda x: x.fillna(method='ffill'))


    # get latest date for each country in OWID dataset
    latest_owid_dates = owid_data.groupby('iso_code')['date'].max().to_frame()

    # merge full date 
    latest_owid_data = pd.merge(latest_owid_dates,owid_data,on=['iso_code','date'])

    # get WHO data
    who_data = pd.read_csv("https://covid19.who.int/who-data/vaccination-data.csv")

    # get PTC owid source classifications
    owid_sources = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vTDKyIaQVtTIy7kn5pD2W8oKM3YoX3YOdSsH3q-r0INH2axjQl6YxgDHBi4HikKx_cmRElde_E-2vlr/pub?gid=2040574494&single=true&output=csv").filter(['Code','OWID Vax Source Category'])

    # merge latest OWID data with WHO data
    merged_data = pd.merge(latest_owid_data,who_data, how='inner', left_on='iso_code', right_on='ISO3').drop(columns=[
        'WHO_REGION','TOTAL_VACCINATIONS_PER100', 'PERSONS_VACCINATED_1PLUS_DOSE_PER100','PERSONS_FULLY_VACCINATED_PER100',
        'VACCINES_USED', 'FIRST_VACCINE_DATE', 'NUMBER_VACCINES_TYPES_USED', 'PERSONS_BOOSTER_ADD_DOSE_PER100',
        'PERSONS_BOOSTER_ADD_DOSE','PERSONS_VACCINATED_1PLUS_DOSE','PERSONS_FULLY_VACCINATED','people_vaccinated'
        ])
    merged_data.rename(columns = {'date':'owid_date','DATE_UPDATED':'WHO_DATE',
    'total_vaccinations':'owid_total_vaccinations',
    'people_vaccinated':'owid_people_vaccinated',
    'TOTAL_VACCINATIONS':'WHO_TOTAL_VACCINATIONS',
    'PERSONS_VACCINATED_1PLUS_DOSE':'WHO_PERSONS_VACCINATED_1PLUS_DOSE',
    'PERSONS_FULLY_VACCINATED':'WHO_PERSONS_FULLY_VACCINATED',
    }, inplace = True)

    # merge combined OWID+WHO data source with PTC owid source classifications
    merged_data = pd.merge(merged_data,owid_sources, how='inner', left_on='ISO3', right_on='Code')

    # calculate total vaccines diff
    merged_data['diff_total_vaccinations'] = merged_data.WHO_TOTAL_VACCINATIONS - merged_data.owid_total_vaccinations
    return merged_data

def get_comparison_dataframe(merged_data):

    dates_matching = len( merged_data.loc[merged_data['owid_date'] == merged_data['WHO_DATE']])
    dates_owid_greater = len(merged_data.loc[merged_data['owid_date'] > merged_data['WHO_DATE']])
    dates_owid_lesser = len(merged_data.loc[merged_data['owid_date'] < merged_data['WHO_DATE']])
    matching_total_vaccinations_df = merged_data.loc[merged_data['owid_total_vaccinations'] == merged_data['WHO_TOTAL_VACCINATIONS']]
    totalvax_matching = len( matching_total_vaccinations_df.index)
    total_matching_vax_who = int(matching_total_vaccinations_df['WHO_TOTAL_VACCINATIONS'].sum(axis=0))
    total_matching_vax_owid = int(matching_total_vaccinations_df['owid_total_vaccinations'].sum(axis=0))

    # owid greater
    owid_greater_total_vaccinations_df = merged_data.loc[merged_data['owid_total_vaccinations'] > merged_data['WHO_TOTAL_VACCINATIONS']]
    owid_greater_count = len(owid_greater_total_vaccinations_df)
    owid_greater_who_totalvax = int(owid_greater_total_vaccinations_df['WHO_TOTAL_VACCINATIONS'].sum(axis=0))
    owid_greater_owid_totalvax = int(owid_greater_total_vaccinations_df['owid_total_vaccinations'].sum(axis=0))

    # owid lesser
    owid_lesser_total_vaccinations_df = merged_data.loc[merged_data['owid_total_vaccinations'] < merged_data['WHO_TOTAL_VACCINATIONS']]
    owid_lesser_count = len(owid_lesser_total_vaccinations_df)
    owid_lesser_who_totalvax = int(owid_lesser_total_vaccinations_df['WHO_TOTAL_VACCINATIONS'].sum(axis=0))
    owid_lesser_owid_totalvax = int(owid_lesser_total_vaccinations_df['owid_total_vaccinations'].sum(axis=0))

    totalvax_who = int(merged_data['WHO_TOTAL_VACCINATIONS'].sum(axis=0))
    total_vax_owid= int(merged_data['owid_total_vaccinations'].sum(axis=0))
    total_vax_diff = totalvax_who-total_vax_owid

    # dictionary of lists
    dict = {
            'Date' : [pd.Timestamp.now(tz = 'US/Eastern')],
            'Total countries':[len( merged_data)],
            'Dates - matching': [dates_matching],
            'Dates - OWID greater':[dates_owid_greater],
            'Dates - OWID lesser':[dates_owid_lesser],
            'Total Vax - matching': [totalvax_matching],
            'Total Vax - matching- WHO total':[total_matching_vax_who],
            'Total Vax - matching- OWID total':[total_matching_vax_owid],
            'Total Vax - OWID greater - count':[owid_greater_count],
            'Total Vax - OWID greater - WHO total':[owid_greater_who_totalvax],
            'Total Vax - OWID greater - OWID total':[owid_greater_owid_totalvax],
            'Total Vax - OWID greater - Diff':[owid_greater_owid_totalvax-owid_greater_who_totalvax],
            'Total Vax - OWID lesser - count':[owid_lesser_count],
            'Total Vax - OWID lesser - WHO total':[owid_lesser_who_totalvax],
            'Total Vax - OWID lesser - OWID total':[owid_lesser_owid_totalvax],
            'Total Vax - OWID lesser - Diff':[owid_lesser_who_totalvax-owid_lesser_owid_totalvax],
            'Total Vax - WHO total':[totalvax_who],
            'Total Vax - OWID total':[total_vax_owid],
            'Total Vax - Diff':[total_vax_diff],

            }

    
    df = pd.DataFrame(dict)
    return df


def main(args_list=None):
    if args_list is None:
        args_list = sys.argv[1:]
    args = parser.parse_args(args_list)
    s3 = S3Backup(bucket_name=args.s3_bucket, s3_subfolder=args.s3_subfolder)

    merged_df = get_merged_dataframe();
    file_path = args.temp_dir + '/' + str(datetime.now()).replace(' ','-').replace(':','-').replace('.','-') + '-merged.csv'
    merged_df.to_csv(file_path) 
    
    # upload to S3
    s3.upload_file(file_path, "merged")
    logger.info('Uploaded merged file to S3')

    comparison_df = get_comparison_dataframe(merged_df);
    file_path = args.temp_dir + '/' + str(datetime.now()).replace(' ','-').replace(':','-').replace('.','-') + '-comparison.csv'
    comparison_df.to_csv(file_path) 

    # upload to S3
    s3.upload_file(file_path, "comparison")
    logger.info('Uploaded comparison file to S3')

if __name__ == "__main__":
    main()