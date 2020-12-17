import json
import tkinter as tk

import numpy as np
import pandas as pd
from IPython.display import display
from PIL import Image, ImageTk, ImageDraw


class MapObject:
    def __init__(self, data_dir=".", config_dir="."):
        """
        Constructor
        :param data_dir: directory for the data
        :param config_dir: directory for the configurations
        """
        # Config folders
        self.data_dir = data_dir
        self.config_dir = config_dir
        # Last data in dataframe
        self._df = pd.read_csv(
            "{}/map_data/map_dataframe_per_megye.csv".format(self.data_dir),
            delimiter=",", parse_dates=["date"]
        )
        # Image configurations
        self._box = json.load(open("{}/box.json".format(self.config_dir), "r"))
        self._regions = json.load(open("{}/regions.json".format(self.config_dir), "r"))
        for region in self._regions:
            self._regions[region] = MapObject.box(self._regions[region]["left"], self._regions[region]["top"])
        # Image manipulation
        self._original = None
        self._filename = None
        self._date = None

    def _load_image(self):
        """
        Load map
        :return: None
        """
        self._filename = "{}/map_png/map_{}.png".format(self.data_dir, self._date)
        self._original = Image.open(self._filename)

    def _find_box(self):
        if self._original is not None:
            np_array = np.array(self._original)
            # create a 2D array for the picture
            img = []
            index = 0
            for row in np_array:
                index += 1
                new_row = []
                for col in row:
                    # if col[0] == 255 and col[1] == 255 and col[2] == 255:
                    if col[0] > 250 and col[1] > 250 and col[2] > 250:
                        new_row.append(hex(0xffffff))
                    else:
                        new_row.append(hex(0x000000))
                        # new_row.append(hex(col[0] * 256 * 256 + col[1] * 256 + col[0]))
                img.append(new_row)
            # set the percentage of red
            no_white_limit = 0.01
            # check rows
            top = 0
            bottom = 0
            pointer = 0
            for row in img:
                pointer += 1
                max_no_white_count = 0
                no_white_count = 0
                for pixel in row:
                    if pixel != hex(0xffffff):
                        no_white_count += 1
                    else:
                        no_white_count = 0
                    if no_white_count > max_no_white_count:
                        max_no_white_count = no_white_count
                if max_no_white_count >= len(row) * no_white_limit and top == 0:
                    # Less than x% not consecutive white points
                    top = pointer
                if max_no_white_count >= len(row) * no_white_limit:
                    bottom = pointer
            # check cols
            left = 0
            right = 0
            pointer = 0
            for col in np.transpose(img):
                pointer += 1
                max_no_white_count = 0
                no_white_count = 0
                for pixel in col:
                    if pixel != hex(0xffffff):
                        no_white_count += 1
                    else:
                        no_white_count = 0
                    if no_white_count > max_no_white_count:
                        max_no_white_count = no_white_count
                if max_no_white_count >= len(col) * no_white_limit and left == 0:
                    # Less than x% not consecutive white points
                    left = pointer
                if max_no_white_count >= len(col) * no_white_limit:
                    right = pointer
            return {
                "left": left, "top": top,
                "right": self._original.size[0] - right, "bottom": self._original.size[1] - bottom
            }

    def _get_geometry(self):
        """
        Generate geometry (for window size)
        :return: widthxheight
        """
        return "{width}x{height}".format(width=self._original.size[0], height=self._original.size[1])

    @staticmethod
    def box(x, y):
        width = 16
        height = 8
        return {"left": x - width / 2, "right": 100 - x - width / 2, "top": y - height / 2,
                "bottom": 100 - y - height / 2}

    @staticmethod
    def scale_coord(size, box, box_percent=None):
        """
        Convert the coordinates
        :param size: Image.size (width, height)
        :param box_percent: percentage of the inner box {"left": 0, "right": 0, "top": 0, "bottom": 0}
        :param box: margin of the box {"left": 0, "right": 0, "top": 0, "bottom": 0}
        :return: absolute values of the box
        """
        if box_percent is None:
            box_percent = {
                "left": 0,
                "right": 0,
                "top": 0,
                "bottom": 0
            }
        width, height = size
        margin_hzt = box["left"] + box["right"]
        margin_vrt = box["top"] + box["bottom"]
        left = box["left"] + (width - margin_hzt) * box_percent["left"] / 100
        right = box["right"] + (width - margin_hzt) * box_percent["right"] / 100
        top = box["top"] + (height - margin_vrt) * box_percent["top"] / 100
        bottom = box["bottom"] + (height - margin_vrt) * box_percent["bottom"] / 100
        return left, top, width - right, height - bottom

    def show_box(self, image, draw):
        draw.rectangle(self.scale_coord(
            size=image.size,
            box=self._box
        ), outline=(0, 0, 0))
        return image

    def show_region(self, image, draw, box_percent):
        draw.rectangle(self.scale_coord(
            size=image.size,
            box=self._box,
            box_percent=box_percent
        ), outline=(0, 0, 255))
        return image

    def show_percent(self, image, draw):
        # (left, 0, left, original.size[1])
        for i in range(0, 100, 1):
            if i % 10 == 0:
                color = (0, 255, 0)
            else:
                color = (255, 255, 0)
            draw.line(self.scale_coord(
                size=image.size,
                box=self._box,
                box_percent={"left": i, "right": 100 - i, "top": 0, "bottom": 0}
            ), fill=color)
            draw.line(self.scale_coord(
                size=image.size,
                box=self._box,
                box_percent={"left": 0, "right": 0, "top": i, "bottom": 100 - i}
            ), fill=color)
        return image

    def show(self, date, gui=False, percent=False):
        """
        Show the picture in a window
        :param gui: True to see the image
        :param percent: True to see the grid
        :param date: date in a yyyy-mm-dd format
        :return: None
        """
        self._date = date
        self._load_image()
        if gui:
            # Create main window
            root = tk.Tk(className="Map {} - {}".format(
                self._date, self._df[self._df["date"] == self._date]["confirmed"].sum()
            ))
            root.geometry(self._get_geometry())

            # frame = tk.Frame(root)
            # frame.pack()
            # t_frame = tk.Toplevel(frame)
            # t_frame.wm_title("my Window")
            # t_frame.geometry(self._get_geometry())

            # Create image
            image = self._original
            draw = ImageDraw.Draw(image)
            self._box = self._find_box()
            image = self.show_box(image, draw)
            if percent:
                image = self.show_percent(image, draw)
            for region in self._regions:
                image = self.show_region(image, draw, self._regions[region])
            image_tk = ImageTk.PhotoImage(image)
            # Create canvas for the picture
            cv = tk.Canvas()
            cv.pack(side='top', fill='both', expand='yes')
            cv.create_image(0, 0, image=image_tk, anchor='nw')
            # Show window
            root.mainloop()
        else:
            image = self._original
            draw = ImageDraw.Draw(image)
            self._box = self._find_box()
            image = self.show_box(image, draw)
            if percent:
                image = self.show_percent(image, draw)
            for region in self._regions:
                image = self.show_region(image, draw, self._regions[region])
        return image

    def input_map(self):
        overall = int(input("Sum: "))
        data_json = []
        for region in self._regions:
            display(self._draw_region(region))
            value = input("{}: ".format(region))
            data_json.append([self._date, value, region, "map_{}.png".format(self._date)])
            overall -= int(value)
        if overall != 0:
            if overall > 0:
                print("Missing {}".format(overall))
            else:
                print("Too much {}".format(overall))
        return data_json

    def _draw_region(self, region):
        return self._original.crop(self.scale_coord(self._original.size, self._box, self._regions[region]))
#        return self._original.crop((
#            self._regions[region]["left"] * width / width_org,
#            self._regions[region]["top"] * height / height_org,
#            (width_org - self._regions[region]["right"]) * width / width_org,
#            (height_org - self._regions[region]["bottom"]) * height / height_org
#        ))

#    def scale_coord(size, box, box_percent=None):
#        """
#        Convert the coordinates
#        :param size: Image.size (width, height)
#        :param box_percent: percentage of the inner box {"left": 0, "right": 0, "top": 0, "bottom": 0}
#        :param box: margin of the box {"left": 0, "right": 0, "top": 0, "bottom": 0}
#        :return: absolute values of the box
#        """
