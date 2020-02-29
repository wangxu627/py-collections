import argparse
import csv
import json
import os
import re
import time
from collections import namedtuple
from itertools import tee

from tinydb import Query, TinyDB
from tinydb.middlewares import CachingMiddleware
from tinydb.storages import JSONStorage
from tqdm import tqdm

from stock_request_aio import KEEP_N

DayInfo = namedtuple(
    "DayInfo", ["date", "open", "high", "low", "price", "amount"])

CachingMiddleware.WRITE_CACHE_SIZE = 5000


def generate_base(path_name, db_name):
    with TinyDB(db_name, storage=CachingMiddleware(JSONStorage)) as db:
        db.purge_tables()
        table = db.table('all_stock')

        files = os.listdir(path_name)
        bar = tqdm(total=len(files))
        for file in files:
            bar.update(1)
            with open(os.path.join(path_name, file)) as f:
                obj = json.load(f)
            kline = []
            for item in obj:
                di = DayInfo(*item)
                kline.append((di.date, di.price))
            kline.sort()
            name = os.path.splitext(file)[0]
            table.insert({"name": name, "kline": kline})
        bar.close()


def append_one_day(day_file, db_name):
    with open(day_file, encoding="utf-8") as f,\
            TinyDB(db_name, storage=CachingMiddleware(JSONStorage)) as db:
        table = db.table('all_stock')
        Stock = Query()
        reader = csv.DictReader(f)
        reader, counter = tee(reader, 2)
        bar = tqdm(total=len(list(counter)))
        for row in reader:
            bar.update(1)
            date = re.split(r"[^\d]", day_file)[0]
            code = row["code"]
            price = row["last_px"]
            info = [int(date), float(price)]
            result = table.get(Stock.name == code)
            if result:
                # zero not included
                if(info[1] != 0):
                    result["kline"].append(info)
                    result["kline"] = result["kline"][-KEEP_N:]
                    table.write_back([result])
            else:
                kline = [info]
                table.insert({"name": code, "kline": kline})
        bar.close()


def search_last_n(last_n, up_or_down, db_name):
    def ups(diff): return diff >= 0
    def downs(diff): return diff <= 0
    opertor = up_or_down and ups or downs

    def print_data(all_data):
        for data in all_data:
            data["kline_diff_pct"] = [
                f"{(d * 100):.2f}%" for d in data["kline_diff_pct"]]
            print(f'{data["name"]}, {data["kline_diff_pct"]}')

    def shorten_kline(item):
        item["kline"] = list(reversed(item["kline"][-(last_n + 1):]))
        return item

    def sum_kline(item):
        total = 0
        for n in item["kline_diff_pct"]:
            total += abs(n)
        return (total, len(item["kline_diff_pct"]))

    now = time.perf_counter()
    with TinyDB(db_name, storage=CachingMiddleware(JSONStorage)) as db:
        table = db.table('all_stock')
        last_n_data = list(map(shorten_kline, table))
        for data in last_n_data:
            data["up_down"] = []
            data["kline_diff"] = []
            data["kline_diff_pct"] = []
            for i in range(len(data["kline"]) - 1):
                diff = data["kline"][i][1] - data["kline"][i + 1][1]
                diff_pct = diff / data["kline"][i + 1][1]
                data["kline_diff"].append(diff)
                data["kline_diff_pct"].append(diff_pct)
                data["up_down"].append(opertor(diff))

        filtered_data = list(
            filter(lambda data: all(data["up_down"]), last_n_data))
        filtered_data_sorted = sorted(
            filtered_data, key=sum_kline, reverse=True)
        take_time = (time.perf_counter() - now) * 1000
        print_data(filtered_data_sorted)
        print(f"take {take_time:.2f}ms to search")


if __name__ == '__main__':
    # parser.add_argument("-d", "--database", type=str)
    # group_generate = parser.add_argument_group('generate')
    # group_generate.add_argument("-g", "--generate", action="store_true")
    # group_generate.add_argument("-f", "--folder", type=str)
    # group_append = parser.add_argument_group('append')
    # group_append.add_argument("-a", "--append", action="store_true")
    # group_append.add_argument("-t", "--today", type=str)
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-g", "--generate", type=str, metavar=('folder','database'), nargs=2, help="input folder and database name")
    group.add_argument("-a", "--append", type=str, metavar=('file','database'), nargs=2, help="input file and database name")
    group.add_argument("-s", "--search", metavar=('days', 'up or down','database'), nargs=3, help="input search condition")
    args = parser.parse_args()
    if args.generate:
        print(args.generate)
        generate_base(*args.generate)
    elif args.append:
        print(args.append)
        append_one_day(*args.append)
    elif args.search:
        print(args.search)
        search_last_n(int(args.search[0]), bool(
            int(args.search[1])), args.search[2])
