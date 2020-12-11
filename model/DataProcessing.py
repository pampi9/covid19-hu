import json
import os
import re

import jsonschema
# from sklearn.linear_model import LogisticRegression
import matplotlib.dates as dates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pandas.plotting import register_matplotlib_converters
from sklearn.linear_model import LinearRegression

register_matplotlib_converters()


class Preprocessing:
    MAPPING = {
        "Elhunyt value": "Elhunytak value",
        "Fertőzött value": "Fertőzöttek value",
        "Gyógyult value": "Gyógyultak value"
    }
    PATHS_TO_JSON = {
        'kpi_hu/': [
            "deaths", "infected", "recovered",
            "deaths_pest", "infected_pest", "recovered_pest",
            "deaths_other", "infected_other", "recovered_other",
            "lockeddown", "tests", "source", "update"],
        'kpi_world/': [
            "deaths_global", "infected_global", "recovered_global",
            "source", "update"]
    }

    def __init__(self):
        self.df = {}

    @staticmethod
    def valid_consolidated(filename):
        """
        Check if the filename is a consolidated one
        :param filename: name of the file to check
        :return: True if a consolidated filename
        """
        return re.match("[0-9]{4}.[0-9]{2}.json", filename) is not None

    @staticmethod
    def preprocess(data_dir):
        # Merge new files
        for path_to_json in Preprocessing.PATHS_TO_JSON:
            path_to_json = "{}/{}".format(data_dir, path_to_json)
            json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]
            output = {}  # one key per month
            for json_file_name in json_files:
                # print(path_to_json+json_file_name)
                file_path = "{}{}".format(path_to_json, json_file_name)
                with open(file_path, 'r') as json_file:
                    data = json.load(json_file)
                    if isinstance(data, dict):
                        if "update" in data:  # single data
                            month = data["update"].split(" ")[0][0:7]
                            if month not in output:
                                output[month] = {}
                            output[month][data["update"]] = data
                            os.remove(file_path)
                        else:
                            month = list(data)[0].split(" ")[0][0:7]
                            if month not in output:
                                output[month] = {}
                            output[month] = {**data, **output[month]}
            for month in dict(sorted(output.items())):
                print("{} - {}: {} db".format(path_to_json, month, len(output[month])), end="\r")
                with open("{}{}.json".format(path_to_json, month), 'w') as outfile:
                    json.dump(output[month], outfile, sort_keys=True)
        print("Preprocess finished")

    def load_existing_data(self, data_dir):
        # Load existing data to Dataframe
        self.df = {}
        for path_to_json, columns in Preprocessing.PATHS_TO_JSON.items():
            path = "{}/{}".format(data_dir, path_to_json)
            key_for_folder = path_to_json.replace("/", "")
            json_files = [
                pos_json for pos_json in os.listdir(path) if
                pos_json.endswith('.json') and self.valid_consolidated(pos_json)
            ]

            self.df[key_for_folder] = None
            for file in json_files:
                print("File:", file, end="\r")
                temp = pd.read_json(path + file, orient="index")
                if isinstance(self.df[key_for_folder], pd.DataFrame):
                    self.df[key_for_folder] = self.df[key_for_folder].append(temp, sort=False)
                else:
                    self.df[key_for_folder] = temp
            self.df[key_for_folder]["update"] = self.df[key_for_folder]["update"].apply(
                lambda cell: pd.Timestamp(Preprocessing.correct_date(cell))
            )
            # Rename with mapping by name
            columns_to_rename = {}
            for col_from, col_to in Preprocessing.MAPPING.items():
                if col_from in self.df[key_for_folder].columns:
                    columns_to_rename[col_from] = col_to
            self.df[key_for_folder].rename(columns=columns_to_rename, inplace=True)
            # Add rates (deaths/recovered)
            self.df[key_for_folder] = self.df[key_for_folder].sort_values("update", ascending=True)
            self.df[key_for_folder] = self.df[key_for_folder][columns]
            self.df[key_for_folder]["Country/Region"] = key_for_folder

        # DETAILS
        with open('{}/details_HU.json'.format(data_dir), 'r') as my_file:
            data_json = json.load(my_file)
        with open('{}/schema.json'.format(data_dir), 'r') as my_file:
            schema_json = json.load(my_file)
        jsonschema.validate(data_json, schema_json)
        df = pd.json_normalize(data_json)
        # Remove template (source='')
        df = df[df["source"] != ""]
        # Convert date column and add week/dayofweek
        df["date"] = pd.to_datetime(df["date"], format='%Y-%m-%d')
        self.df["details"] = df

    def add_aggregation(self):
        if "kpi_hu" in self.df:
            # HU aggregation
            if all(column in self.df["kpi_hu"].columns
                   for column in ["recovered", "recovered_pest", "recovered_other"]):
                self.df["kpi_hu"]["recovered"] = self.df["kpi_hu"].apply(
                    lambda row:
                    row["recovered_pest"] + row["recovered_other"] if pd.isnull(row['recovered']) else row['recovered'],
                    axis=1
                )
            if all(column in self.df["kpi_hu"].columns
                   for column in ["deaths", "deaths_pest", "deaths_other"]):
                self.df["kpi_hu"]["deaths"] = self.df["kpi_hu"].apply(
                    lambda row:
                    row["deaths_pest"] + row["deaths_other"] if pd.isnull(row['deaths']) else row['deaths'],
                    axis=1
                )
            if all(column in self.df["kpi_hu"].columns
                   for column in ["infected", "infected_pest", "infected_other"]):
                self.df["kpi_hu"]["infected"] = self.df["kpi_hu"].apply(
                    lambda row:
                    row["infected_pest"] + row["infected_other"] + row["recovered"] + row["deaths"]
                    if pd.isnull(row['infected']) else
                    row['infected'],
                    axis=1
                )

    @staticmethod
    def correct_date(date_time):
        # Correct bad formatting of datetime (from webpage)
        split_datetime = date_time.split(" ")
        if len(split_datetime) == 1:
            return date_time
        else:
            split_datetime[1] = split_datetime[1].replace(".", ":")
            return " ".join(split_datetime)


class Analyse:
    @staticmethod
    def start_analyse(df, countries, y_label, y_column):
        df = df[(df[y_column] > 0)]  # suppress data with no cases (before begin of epidemy)
        X = {}
        converted_x = {}
        y = {}
        y_log = {}
        regressor = {}
        model = {}
        r_sq = {}
        x_pred = {}
        y_pred = {}

        coeff = {}
        daily_coeff = {}
        doubling_coeff = {}

        for country in countries:
            # Select data
            X[country] = df[df["Country/Region"] == country][["update"]].copy()
            converted_x[country] = X[country].copy()
            converted_x[country]["update"] = converted_x[country]["update"].astype(int)
            # for backwards conversion to datetime: pd.to_datetime(X["update"].astype(int))
            y[country] = df[df["Country/Region"] == country][y_column]
            y_log[country] = np.log(df[df["Country/Region"] == country][y_column])
            # Create model
            regressor[country] = LinearRegression()
            model[country] = regressor[country].fit(converted_x[country], y_log[country])
            r_sq[country] = model[country].score(converted_x[country], y_log[country])
            coeff[country] = regressor[country].coef_[0]
            daily_coeff[country] = round(10 ** (3600 * 24 * (10 ** 9) * regressor[country].coef_[0]), 1)
            doubling_coeff[country] = 2 / daily_coeff[country]

            # Generate prediction: regressor.predict(converted_x)
            x_pred[country] = pd.DataFrame([
                X[country].min(),
                X[country].min() + pd.Timedelta(1, unit='d'),
                X[country].min() + pd.Timedelta(2, unit='d'),
                X[country].min() + pd.Timedelta(3, unit='d'),
                X[country].min() + pd.Timedelta(4, unit='d'),
                X[country].min() + pd.Timedelta(5, unit='d'),
                X[country].min() + pd.Timedelta(6, unit='d'),
                X[country].min() + pd.Timedelta(7, unit='d'),
                X[country].max() + pd.Timedelta(14, unit='d')])
            y_pred[country] = np.exp(regressor[country].predict(x_pred[country].astype(int)))
        # Plot result
        fig, ax = plt.subplots(ncols=1, nrows=1)
        ax.set_yscale("log")
        # ax.set_ylim(1,10000)

        ax.grid(True, which="minor", axis="y", color='g', linestyle='--', linewidth=1)
        ax.grid(True, which="major", axis="y", color='g', linestyle='-', linewidth=2)

        ax.xaxis.set_minor_locator(dates.DayLocator(bymonthday=range(1, 32), interval=1))
        ax.xaxis.set_minor_formatter(dates.DateFormatter('%d'))
        ax.xaxis.set_major_locator(dates.WeekdayLocator(byweekday=0))
        ax.xaxis.set_major_formatter(dates.DateFormatter('\n%m-%d'))
        ax.grid(True, which="major", axis="x", color='g', linestyle='-', linewidth=2)
        fig.set_size_inches(20, 5)
        for country in countries:
            plot_df = pd.DataFrame(X[country])
            plot_df["y"] = y[country]
            plot_df.plot(x="update", y="y", ax=ax, style="o:", label="act " + country)

            plot_df_pred = pd.DataFrame(x_pred[country])
            plot_df_pred["y"] = y_pred[country]
            plot_df_pred.plot(x="update", y="y", ax=ax, style="--", label="pred " + country)
        plt.title('prediction')
        plt.xlabel('datetime')
        plt.ylabel(y_label)
        plt.show()

        print('Coefficient of determination:', r_sq)
        print("Evolution factor per day: {}".format(daily_coeff))
        print("Doubling in day(s): {}".format(doubling_coeff))

        x_pred[country]["pred"] = pd.DataFrame(y_pred[country])
        x_pred[country]["prev"] = x_pred[country].apply(
            lambda row: x_pred[country][x_pred[country]["update"] < row["update"]]["update"].max(),
            axis=1
        )
        x_pred[country]["diff"] = x_pred[country].apply(
            lambda row: row["pred"] / x_pred[country][x_pred[country]["update"] == row["prev"]]["pred"].max(),
            axis=1
        )
        return x_pred
