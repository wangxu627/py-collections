import os
import re
import json
import asyncio
from urllib.parse import urlparse
from html import unescape

import aiohttp
from bs4 import BeautifulSoup



SCRIPT_VERSION = '1.0'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.25 Safari/537.36',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'zh-CN,zh-TW;q=0.8,zh;q=0.6,en;q=0.4,ja;q=0.2',
    'cache-control': 'max-age=0'
}


async def fetch(session, url):
    async with session.get(url, headers=HEADERS, verify_ssl=False) as response:
        return await response.read()


async def download_one(url):
    async with aiohttp.ClientSession() as session:
        await parse(session, url)


async def download_all(urls):
    tasks = []
    async with aiohttp.ClientSession() as session:
        for url in urls:
            tasks.append(asyncio.create_task(parse(session, url)))
        await asyncio.gather(*tasks)


async def parse(session, url):
    try:
        print('Parsing...')
        page = await fetch(session, url)
        page = page.decode()
        soup = BeautifulSoup(page, 'html.parser')
        model_id = urlparse(url).path.split('/')[2].split('-')[-1]
        data = unescape(soup.find(id='js-dom-data-prefetched-data').string)
        data = json.loads(data)
        name = validate_title(data['/i/models/'+model_id]['name'])
        thumbnail_data = data['/i/models/'+model_id]['thumbnails']['images']
        thumbnail = get_biggest_image(thumbnail_data)
        osgjs_url = data['/i/models/'+model_id]['files'][0]['osgjsUrl']
        model_file = osgjs_url.replace('file.osgjs.gz', 'model_file.bin.gz')
        textures_data = data['/i/models/'+model_id +'/textures?optimized=1']['results']

        print('Model Id:', model_id)
        print('Name:', name)
        print('Thumbnail URL:', thumbnail)
        print('osgjs URL:', osgjs_url)
        print('Model File:', model_file)
        print('Textures:', len(textures_data))

        tasks = []
        tasks.append(asyncio.create_task(
            download(session, thumbnail, os.path.join(name, 'thumbnail.jpg'))))
        tasks.append(asyncio.create_task(
            download(session, osgjs_url, os.path.join(name, 'file.osgjs'))))
        tasks.append(asyncio.create_task(
            download(session, model_file, os.path.join(name, 'model_file.bin.gz'))))

        for texture in textures_data:
            texture_url = get_biggest_image(texture['images'])
            tasks.append(asyncio.create_task(download(session, texture_url, os.path.join(
                name, 'texture', validate_title(texture['name'])))))

        await asyncio.gather(*tasks)
    except Exception as e:
        print("Error happend : ", e)


def get_biggest_image(images):
    size = 0
    for img in images:
        if img['size'] != None and img['size'] > size:
            size = img['size']
            img_url = img['url']
    return img_url


def validate_title(title):
    pattern = r'[\\/:*?"<>|\r\n]+'
    return re.sub(pattern, "_", title)


async def download(session, url, filename):
    print('Downloading:', filename)
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        if os.path.exists(filename):
            if os.path.getsize(filename) > 0:
                print('file exists.')
            else:
                with open(filename, 'wb') as file:
                    content = await fetch(session, url)
                    file.write(content)
        else:
            with open(filename, 'wb') as file:
                content = await fetch(session, url)
                file.write(content)
    except Exception:
        pass


async def ff():
    await asyncio.sleep(1)

if __name__ == '__main__':
    from datasource import collections
    asyncio.run(download_all(collections))
    asyncio.run(ff())
