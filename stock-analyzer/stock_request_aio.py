import argparse
import asyncio
import glob
import itertools
import json
import operator
import os
import re
import sys
import time

import aiohttp
from tqdm import tqdm

KEEP_N = 30


def shorten(src_folder, dst_folder):
    '''
    将第一步收集到的数据取最近的N天
    '''
    os.makedirs(src_folder, exist_ok=True)
    os.makedirs(dst_folder, exist_ok=True)
    files = os.listdir(src_folder)
    bar = tqdm(total=len(files))
    for fn in files:
        bar.update(1)
        if re.match(r"^\d{6}", fn):
            fn_in = os.path.join(src_folder, fn)
            fn_out = os.path.join(dst_folder, fn)

            with open(fn_in) as f, open(fn_out, "w") as fo:
                arr = json.load(f)
                maxsize = min(len(arr), KEEP_N)
                json.dump(arr[-maxsize:], fo)
    bar.close()


def collect_names():
    csv_files = glob.glob("*.csv")
    names_set = set()
    for fn in csv_files:
        try:
            with open(fn, encoding="utf-8") as f:
                cnt = 0
                current_name_set = set()
                for line in itertools.islice(f, 1, sys.maxsize):
                    arr = line.split(",")
                    current_name_set.add((arr[0], arr[4]))
                    cnt += 1
                names_set |= current_name_set
        except Exception:
            print(f"handle file {fn} mistake")
    return names_set


def get_today_format_str():
    t = time.localtime(time.time())
    return time.strftime("%Y%m%d", t)


def merge_to_file(fh, obj, first):
    fmt = ''
    data = obj["data"]["sort"]

    # if k == "fields":
    def gen_fmt():
        fmt = "{0}"
        for idx, _ in enumerate(v, 1):
            fmt += ",{" + str(idx) + "}"
        return fmt

    for k, v in data.items():
        if fmt == '':
            fmt = gen_fmt()

        # output title line only in first == true
        if first:
            first = False
            fmt_data = fmt.format("code", *data["fields"])
            print(fmt_data, file=fh)

        if k != "fields":
            fmt_data = fmt.format(k, *v)
            print(fmt_data, file=fh)


async def fetch(session, url):
    async with session.get(url) as response:
        text = await response.text()
        # print(text)
        return json.loads(text)


async def request_daily():
    async with aiohttp.ClientSession() as session:
        url = 'http://zzw.hsmdb.com/iwin_zzbweb-webapp/quote/v1/sort?en_hq_type_code=SS.ESA.M,SZ.ESA&sort_field_name=px_change_rate&data_count={}&sort_type=1&fields=prod_name,last_px,business_amount,current_amount,preclose_px,open_px,high_px,low_px,vol_ratio,business_balance,px_change,hq_type_code,px_change_rate&start_pos={}'
        page_size = 100
        page_count = 39

        name = get_today_format_str()
        start_time = time.perf_counter()
        tasks = []
        for i in range(page_count):
            req_url = url.format(page_size, i * page_size)
            task = asyncio.create_task(fetch(session, req_url))
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        with open(f"{name}_aio.csv", "w", encoding="utf-8") as f:
            for i, result in enumerate(results):
                merge_to_file(f, result, i == 0)
        print("Cost time : ", time.perf_counter() - start_time)


async def request_kline(names):
    async with aiohttp.ClientSession() as session:
        url = "http://zzw.hsmdb.com/iwin_zzbweb-webapp/quote/v1/kline?get_type=range&prod_code={}&candle_period=6&fields=open_px,high_px,low_px,close_px,business_amount"

        folder = get_today_format_str()
        os.makedirs(folder, exist_ok=True)

        tasks = []
        for name in names:
            req_url = url.format(name)
            task = asyncio.create_task(fetch(session, req_url))
            tasks.append(task)
        # results = await asyncio.gather(*tasks)
        # for result in results:
        bar = tqdm(total=len(tasks))
        for future in asyncio.as_completed(tasks):
            result = await future
            bar.update(1)
            candle = result["data"]["candle"]
            name = list(candle.keys())[1]
            with open(f"{folder}/{name}.json", "w", encoding="utf-8") as f:
                f.write(json.dumps(candle[name]))
        bar.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--daily", action="store_true")
    parser.add_argument("-t", "--total", action="store_true")
    args = parser.parse_args()
    if args.daily:
        asyncio.run(request_daily())
    if args.total:
        asyncio.run(request_daily())
        names = collect_names()
        names = map(operator.itemgetter(0), names)
        asyncio.run(request_kline(names))
        print("start to shorten ...")
        name = get_today_format_str()
        shorten(name, name + "_shorten")
