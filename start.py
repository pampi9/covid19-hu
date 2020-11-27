import threading
import logging
import time
import model as md

load = True
show = False


def threaded_load():
    while True:
        data_dir = "data"
        logging.info("%s: starting", "load data")
        md.Load.load_map(data_dir=data_dir)
        md.Load.load_deaths(data_dir=data_dir)
        md.Load.load_kpi(data_dir=data_dir)
        md.Load.load_news(data_dir=data_dir)
        process_thread = threading.Thread(target=md.Load.process_data, args=(data_dir,))
        process_thread.start()
        logging.info("%s: ending", "load data")
        time.sleep(3600)


if __name__ == "__main__":
    date = "2020-11-16"
    log_format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=log_format, level=logging.INFO,
                        datefmt="%H:%M:%S")
    logging.info("Main    : start")
    if load:
        load_thread = threading.Thread(target=threaded_load)
        logging.info("Main    : before running thread")
        load_thread.start()
    if show:
        my_map = md.MapObject(data_dir="data", config_dir="model")
        my_map.show(date=date, gui=True, percent=False)
    logging.info("Main    : wait for the threads to finish")
