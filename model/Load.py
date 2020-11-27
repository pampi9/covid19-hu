import urllib3
import json
import requests
import datetime
from bs4 import BeautifulSoup
import urllib
import os
import shutil
import filecmp
from .NewsExtractor import NewsExtractor
from .DataProcessing import Preprocessing


class Load:
    @staticmethod
    def load_map(data_dir="."):
        # map
        url_map = "https://koronavirus.gov.hu/terkepek/fertozottek"
        page_map = requests.get(url_map, timeout=2.50)
        soup_map = BeautifulSoup(page_map.content, 'html.parser')
        img_map = soup_map.select("img")[0]
        if img_map.has_attr('src'):
            url_map_img = img_map['src']
            print(url_map_img)
            file_name_tmp = "{}/map_png/tmp_terkep.png".format(data_dir)
            file_name_latest = "{}/map_png/latest_terkep.png".format(data_dir)
            file_name_time = "{}/map_png/map_{}.png".format(data_dir, datetime.date.today())
            # Download the file from `url` and save it locally under `file_name_tmp`:
            with urllib.request.urlopen(url_map_img) as response, open(file_name_tmp, 'wb') as out_file:
                data = response.read()  # a `bytes` object
                out_file.write(data)
            # If latest missing copy tmp to latest
            if os.path.isfile(file_name_tmp):
                # Latest missing - copy tmp to latest and to new file
                if not os.path.isfile(file_name_latest):
                    shutil.copyfile(file_name_tmp, file_name_latest)
                    print("Map saved as {}!".format(file_name_time))
                    shutil.copyfile(file_name_tmp, file_name_time)
                # Tmp differs from latest
                elif not filecmp.cmp(file_name_tmp, file_name_latest):
                    # New file missing - copy to latest and to new file
                    if not os.path.isfile(file_name_time):
                        print("Map saved as {}!".format(file_name_time))
                        shutil.copyfile(file_name_tmp, file_name_time)
                    print("Map changed!")
                    shutil.copyfile(file_name_tmp, file_name_latest)
        else:
            print("Map img is missing!")

    @staticmethod
    def load_deaths(data_dir="."):
        # detailed deaths
        try:
            values_deaths = Load._collect_deaths()
            deaths_count = len(values_deaths)

            with open("{}/deaths_hu/latest.json".format(data_dir), "r") as infile:
                previous_state = json.loads(infile.read())
            if values_deaths != previous_state:
                print("Saved into {}".format("{}/HU_deaths/{}.json".format(data_dir, datetime.datetime.now())))
                with open("{}/deaths_hu/{}.json".format(data_dir, datetime.datetime.now()), "w") as outfile:
                    json.dump(values_deaths, outfile)
            with open("{}/deaths_hu/latest.json".format(data_dir), "w") as outfile:
                json.dump(values_deaths, outfile)
        except urllib3.exceptions.ReadTimeoutError as e:
            print("ReadTimeoutError: load_deaths")
        except urllib3.exceptions.MaxRetryError as e:
            print("MaxRetryError: load_deaths")
        except requests.exceptions.ConnectTimeout as e:
            print("ConnectTimeout: load_deaths")

    @staticmethod
    def _collect_deaths():
        url_deaths = "https://koronavirus.gov.hu/elhunytak"
        continue_flag = True
        header_deaths = {}
        values_deaths = []

        while continue_flag:
            print(url_deaths)
            # detailed deaths
            # TODO: try except
            page_deaths = requests.get(url_deaths, timeout=2.50)
            soup_deaths = BeautifulSoup(page_deaths.content, 'html.parser')
            rows_deaths = soup_deaths.select("table")[0].select("tr")

            cells = [
                "views-field-field-elhunytak-sorszam",
                "views-field-field-elhunytak-nem",
                "views-field-field-elhunytak-kor",
                "views-field-field-elhunytak-alapbetegsegek"
            ]
            for row in rows_deaths:
                if len(header_deaths) == 0:
                    for cell in cells:
                        header_deaths[cell] = row.select("th.{}".format(cell))[0].text.replace("\n", "").strip()
                else:
                    value_deaths = {}
                    for cell in cells:
                        if len(row.select("td.{}".format(cell))) > 0:
                            value_deaths[header_deaths[cell]] = row.select(
                                "td.{}".format(cell)
                            )[0].text.replace("\n", "").strip()
                    values_deaths.append(value_deaths)
            if len(soup_deaths.select("li.next a")) > 0:
                url_deaths = "https://koronavirus.gov.hu" + soup_deaths.select("li.next a")[0]["href"]
            else:
                continue_flag = False
        return values_deaths

    @staticmethod
    def load_kpi(data_dir="."):
        api_keys = {
            "Magyarországon": {
                "infected_pest": "div#api-fertozott-pest",
                "infected_other": "div#api-fertozott-videk",
                "recovered_pest": "div#api-gyogyult-pest",
                "recovered_other": "div#api-gyogyult-videk",
                "deaths_pest": "div#api-elhunyt-pest",
                "deaths_other": "div#api-elhunyt-videk",
                "lockeddown": "div#api-karantenban",
                "tests": "div#api-mintavetel"
            },
            "A világban": {
                "deaths_global": "div#api-elhunyt-global",
                "infected_global": "div#api-fertozott-global",
                "recovered_global": "div#api-gyogyult-global"
            }
        }

        url_hu = "https://koronavirus.gov.hu/"
        page = requests.get(url_hu, timeout=2.50)
        soup = BeautifulSoup(page.content, 'html.parser')
        json_output = Load.extract_json(soup, api_keys)

        update = []
        source = []
        for api_key in api_keys:
            update.append(json_output[api_key]["update"])
            source.append(json_output[api_key]["source"])

        print([update[0], source[0], update[1], source[1]])

        # Output creation HU
        file_output = json_output["Magyarországon"]
        # Check identity of data
        check = True
        if os.path.isfile("{}/kpi_hu/{}.json".format(data_dir, update[0])):
            with open("{}/kpi_hu/{}.json".format(data_dir, update[0]), 'r') as infile:
                content = json.load(infile)
                if content != file_output:
                    check = False
        # File writing
        if check:
            with open("{}/kpi_hu/{}.json".format(data_dir, update[0]), 'w') as outfile:
                json.dump(file_output, outfile)
        else:
            with open("{}/kpi_hu/{}_2.json".format(data_dir, update[0]), 'w') as outfile:
                json.dump(file_output, outfile)

        # Output creation World
        file_output = json_output["A világban"]
        # Check identity of data
        check = True
        if os.path.isfile("{}/kpi_world/{}.json".format(data_dir, update[1])):
            with open("{}/kpi_world/{}.json".format(data_dir, update[1]), 'r') as infile:
                content = json.load(infile)
                if content != file_output:
                    check = False
        # File writing
        if check:
            with open("{}/kpi_world/{}.json".format(data_dir, update[1]), 'w') as outfile:
                json.dump(file_output, outfile)
        else:
            with open("{}/kpi_world/{}_2.json".format(data_dir, update[1]), 'w') as outfile:
                json.dump(file_output, outfile)

    @staticmethod
    def convert_str_to_number(text):
        """ Convert Str to Int """
        return int(text.replace(" ", ""))

    @staticmethod
    def extract_date_from_text(text):
        """ Extract the date part of the Str """
        return text.replace("Legutolsó frissítés dátuma: ", "").strip()

    @staticmethod
    def extract_source_from_text(text):
        """ Extract the source part of the Str """
        return text.replace("Forrás: ", "").strip()

    @staticmethod
    def extract_info(soup, h2):
        """ Extract the info (source/update) """
        blocs = soup.select("div.bg-even.nosides")
        update_found = "ERROR: missing update time for {}".format(h2)
        source_found = "ERROR: missing source for {}".format(h2)
        for bloc in blocs:
            titles = bloc.select("div.well-lg.text-center")
            sources = bloc.select("p")
            if len(bloc.select("h2")) > 0:
                title_found = bloc.select("h2")[0].text
                if title_found == h2 and len(sources) == 2:
                    update_found = sources[0].text
                    update_found = Load.extract_date_from_text(update_found)
                    source_found = sources[1]
                    if len(source_found.select("small")) > 0:
                        source_found = source_found.select("small")[0].text
                    else:
                        source_found = source_found.text
                    source_found = Load.extract_source_from_text(source_found)
        return {"update": update_found, "source": source_found}

    @staticmethod
    def extract_json(soup, api_keys):
        """ Create the json based on the 'soup' """
        json_output = {}
        for index in api_keys:
            # Extract Source/Update
            json_output[index] = Load.extract_info(soup, index)
            # Extract numbers from 'API'
            for api_key in api_keys[index]:
                json_output[index][api_key] = Load.convert_str_to_number(soup.select(api_keys[index][api_key])[0].text)
        return json_output

    @staticmethod
    def load_news(data_dir="."):
        # Extract news data
        NewsExtractor(1, data_dir)

    @staticmethod
    def process_data(data_dir="."):
        try:
            my_process = Preprocessing()
            my_process.preprocess(data_dir=data_dir)
            my_process.load_existing_data(data_dir=data_dir)
            my_process.add_aggregation()

        except Exception as e:
            print(e)
