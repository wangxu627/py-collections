import argparse
import asyncio
import ntpath
import os
import struct
import sys
from enum import Enum

from tqdm import tqdm

'''
GET:
-------------------
| CMD(1) | IDX(1) |
--------------------
--------------------------------------------------------------------
| FILENAME_LEN(4) | FILENAME_DATA(N) | FILE_SIZE(4) | FILE_DATA(N) |
--------------------------------------------------------------------

LIST:
----------
| CMD(1) |
----------
----------------------------
| STR_LEN(4) | STR_DATA(N) |
----------------------------

SEND:
-----------------------------------------------------------------------------
| CMD(1) | FILE_SIZE(4) | FILENAME_LEN(4) | FILENAME_DATA(N) | FILE_DATA(N) |
-----------------------------------------------------------------------------
--------
| NONE |
--------
'''

PORT = 11199
SERVER_DFT_IP = "0.0.0.0"
CLIENT_DFT_IP = "localhost"


class CommandCode(Enum):
    CMD_GET_FILE = 1
    CMD_LIST_FILE = 2
    CMD_SEND_FILE = 3
    CMD_SEND_CLOSE = 4


class Command(Enum):
    CMD_GET_FILE = "get"
    CMD_LIST_FILE = "list"
    CMD_SEND_FILE = "send"
    CMD_SEND_CLOSE = "close"


CHUNK_SIZE = 4 * 1024 * 1024

g_list_info = {}


def write_data(f, data):
    return f.write(data)


def read_data(f, size):
    return f.read(size)


async def get_file(reader, writer):
    data = await reader.readexactly(1)
    index = struct.unpack(">b", data)[0]
    filename = g_list_info[id(reader)][index - 1]
    filesize = os.path.getsize(filename)
    filename_b = filename.encode()
    data = struct.pack(">i", len(filename_b))
    writer.write(data)
    writer.write(filename_b)
    data = struct.pack(">i", filesize)
    writer.write(data)
    await writer.drain()
    with open(filename, "rb") as f:
        while(1):
            data = f.read(CHUNK_SIZE)
            if(not data):
                break
            writer.write(data)
            await writer.drain()


async def list_file(reader, writer):
    files = os.listdir()
    g_list_info[id(reader)] = files
    data = "/".join(files)
    data = data.encode()
    size = struct.pack(">i", len(data))
    writer.write(size)
    writer.write(data)
    await writer.drain()


async def send_file(reader, writer):
    loop = asyncio.get_event_loop()
    data = await reader.readexactly(4)
    filesize = struct.unpack(">i", data)[0]
    data = await reader.readexactly(4)
    size = struct.unpack(">i", data)[0]
    data = await reader.read(size)
    filepath = data.decode()
    filename = ntpath.basename(filepath)
    n = 0
    with open(filename, "wb") as f:
        while n < filesize:
            data = await reader.read(CHUNK_SIZE)
            read_size = len(data)
            n += read_size
            await loop.run_in_executor(None, write_data, f, data)


async def handle_read_data(reader, writer):
    print("Client connected")
    while(1):
        try:
            cmd = await reader.read(1)
            cmd = CommandCode(struct.unpack(">b", cmd)[0])
            print("Get cmd : ", cmd)
            if(cmd == CommandCode.CMD_GET_FILE):
                await get_file(reader, writer)
            elif(cmd == CommandCode.CMD_LIST_FILE):
                await list_file(reader, writer)
            elif(cmd == CommandCode.CMD_SEND_FILE):
                await send_file(reader, writer)
            elif(cmd == CommandCode.CMD_SEND_CLOSE):
                reader.close()
                writer.close()
                break
            else:
                writer.write(b'Unrecognized comomand')
                await writer.drain()
        except Exception as e:
            print("Close this connection")
            print(str(e))
            break
    print("Client disconnected")
    if(id(reader) in g_list_info):
        del g_list_info[id(reader)]


async def create_server(address, port, loop):
    server = await asyncio.start_server(handle_read_data, address, port, loop=loop)
    return server


def run_server(port=PORT):
    loop = asyncio.get_event_loop()
    server = loop.run_until_complete(create_server(SERVER_DFT_IP, port, loop))
    host = server.sockets[0].getsockname()
    print('Serving on {}. Hit CTRL-C to stop.'.format(host))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    print('Server shutting down.')
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


async def get_request(reader, writer, index):
    cmd = CommandCode.CMD_GET_FILE
    data = struct.pack(">b", cmd.value)
    writer.write(data)
    data = struct.pack(">b", int(index))
    writer.write(data)
    await writer.drain()
    data = await reader.readexactly(4)
    size = struct.unpack(">i", data)[0]
    data = await reader.readexactly(size)
    filename = data.decode()
    data = await reader.readexactly(4)
    size = struct.unpack(">i", data)[0]
    print("Filename size : ", size)
    print("Filename : ", filename)
    print("Filesize : ", size)
    n = 0
    with open(filename, "wb") as f, tqdm(total=size) as bar:
        while(n < size):
            read_data = await reader.read(CHUNK_SIZE)
            read_size = len(read_data)
            n += read_size
            bar.update(read_size)
            f.write(read_data)


def print_file_list(arr):
    for i, a in enumerate(arr, 1):
        print(i, a)


async def list_request(reader, writer):
    cmd = CommandCode.CMD_LIST_FILE
    data = struct.pack(">b", cmd.value)
    writer.write(data)
    await writer.drain()
    data = await reader.readexactly(4)
    size = struct.unpack(">i", data)[0]
    n = 0
    data = bytearray()
    while(n < size):
        read_data = await reader.read(size)
        data.extend(read_data)
        n += len(read_data)
    line = data.decode()
    print_file_list(line.split("/"))


async def send_request(reader, writer, filepath):
    print(filepath)
    if not os.path.exists(filepath):
        print("File is not exists")
        return
    loop = asyncio.get_event_loop()
    cmd = CommandCode.CMD_SEND_FILE
    data = struct.pack(">b", cmd.value)
    writer.write(data)
    filesize = os.path.getsize(filepath)
    data = struct.pack(">i",  filesize)
    writer.write(data)
    filepath_b = filepath.encode()
    data = struct.pack(">i",  len(filepath_b))
    writer.write(data)
    writer.write(filepath_b)
    await writer.drain()
    print(f"Prepare to send file:{filepath_b}")
    with open(filepath, "rb") as f, tqdm(total=filesize) as bar:
        while(1):
            data = await loop.run_in_executor(None, read_data, f, CHUNK_SIZE)
            if(not data):
                break
            bar.update(len(data))
            writer.write(data)
            await writer.drain()


async def create_connect(address, port, loop):
    reader, writer = await asyncio.open_connection(address, port, loop=loop)
    while(1):
        sys.stdout.write("Type command : ")
        sys.stdout.flush()
        cmd = sys.stdin.readline()
        if(cmd.strip() == ""):
            continue
        try:
            cmds = cmd.split()
            if(len(cmds) > 1):
                cmds[1] = " ".join(cmds[1:]).strip('"')

            cmd = Command(cmds[0])
            if(cmd == Command.CMD_GET_FILE):
                await get_request(reader, writer, cmds[1])
            elif(cmd == Command.CMD_LIST_FILE):
                await list_request(reader, writer)
            elif(cmd == Command.CMD_SEND_FILE):
                await send_request(reader, writer, cmds[1])
            elif(cmd == Command.CMD_SEND_CLOSE):
                exit()
        except IndexError:
            print("Should provide rightful command argument")
        except Exception as e:
            print(str(e))


def run_client(address, port=PORT):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(create_connect(address, port, loop))
    print('Client shutting down.')
    loop.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="File transfer tool")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-s", "--server", type=int, nargs="?",
                       const=PORT, help="listening server port")
    group.add_argument("-c", "--client", nargs='+',
                       help="server ip and port that will connect to")
    args = parser.parse_args()
    if(args.server):
        run_server(args.server)
    elif(args.client):
        run_client(*args.client[:2])
