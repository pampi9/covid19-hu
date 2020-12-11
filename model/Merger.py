# install pyarrow with: `pip install pyarrow`

import os

import pandas as pd
import dask.dataframe as dd
from .DataProcessing import Preprocessing


class Merger:
    GZIP_PATTERN = "{}/aggregated/{}.gzip"
    JSON_PATTERN = "{}/aggregated/{}.json"

    def __init__(self, data_dir="data", load_from_raw=True, write_agg=False, load_from_agg=False):
        self.data_dir = data_dir
        self.df = {}
        if load_from_raw:
            self.load_raw_data()
        if write_agg:
            self.write_agg_data()
        if load_from_agg:
            self.load_agg_data()

    def load_raw_data(self):
        my_processing = Preprocessing()
        my_processing.load_existing_data(data_dir=self.data_dir)
        my_processing.add_aggregation()
        self.df = my_processing.df

    def write_agg_data(self):
        for index in self.df:
            self.df[index].to_parquet(
                self.GZIP_PATTERN.format(self.data_dir, index),
                compression='gzip',
                engine='pyarrow'
            )

    def load_agg_data(self):
        for index in ["kpi_hu", "kpi_world", "details"]:
            self.df[index] = pd.read_parquet(self.GZIP_PATTERN.format(self.data_dir, index))

    def show_kpi_hu(self):
        return self.df["kpi_hu"]

    def show_kpi_world(self):
        return self.df["kpi_world"]

    def convert_deaths_df(self):
        folder = "{}/deaths_hu".format(self.data_dir)
        files = os.scandir(folder)
        file_names = []
        for file in files:
            if file.name != "latest.json" and file.name[-5:] == ".json":
                file_names.append(file.name)
        file_names.sort()

        df0 = DataFrameExtended.json2df(folder, file_names[0])
        max_index = 0
        for key in range(0, len(file_names)):
            df = DataFrameExtended.json2df(folder, file_names[key], max_index)
            df.to_parquet(
                self.GZIP_PATTERN.format(self.data_dir, "deaths_{}".format(file_names[key][0:10])),
                compression='gzip',
                engine='pyarrow'

            )
        return df0.reset_index()

    def merge_deaths_df(self):
        return dd.read_parquet(self.GZIP_PATTERN.format(self.data_dir, "deaths_*"))


class DataFrameExtended:
    @staticmethod
    def json2df(folder, filename, start_index=0):
        df = pd.read_json("{}/{}".format(folder, filename)).dropna()
        df["Sorszám"] = df["Sorszám"].astype(int)
        df["Kor"].replace('', None, inplace=True)
        df["Kor"] = df["Kor"].astype(int)
        df["Nem"] = df["Nem"].str[0:1].str.upper()
        df["date"] = pd.to_datetime(filename[0:10], format='%Y-%m-%d')
        return df[df["Sorszám"] > start_index]
