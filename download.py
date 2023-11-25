import requests
import json
import numpy as np
import pandas as pd
import os

import nest_asyncio
nest_asyncio.apply()

import asyncio
import aiohttp

query = """query {
  pool(id: "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640") {
    createdAtBlockNumber
    token0 {
      symbol
    }
    token1 {
      symbol
    }
  }
}"""

url = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3'
r = requests.post(url, json={'query': query})

if r.status_code == 200:
  json_data = json.loads(r.text)

created_at_block_number = json_data['data']['pool']['createdAtBlockNumber']
file_path = 'swaps.csv'
final_line = None
errors_count = 0
plus = 10000

if os.path.exists(file_path):
  with open(file_path, "r", encoding="utf-8", errors="ignore") as scraped:
    final_line = scraped.readlines()[-1]

if final_line is not None:
  final_block = int(final_line.split(',')[0]) + 1
else:
  final_block = created_at_block_number

arr = np.empty((0,5), int)

async def fetch(session, number):
  global arr, errors_count

  block_number = number + final_block

  query = """query {
    pool(id: "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640", block: {number: %d}) {
      volumeToken0
      volumeToken1
      txCount
      token0Price
    }
  }""" % (block_number)

  url = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3'
  try:
    async with session.post(url, json={'query': query}) as resp:
      if resp.status == 200:
        if block_number % 10000 == 0:
          print(f"{block_number:,}", "/", f"{int((final_block + plus) / 10000) * 10000:,}")
        j = await resp.json()
        if 'data' in j:
          arr = np.append(arr, np.array([[
              block_number,
              float(j['data']['pool']['volumeToken0']),
              float(j['data']['pool']['volumeToken1']),
              int(j['data']['pool']['txCount']),
              float(j['data']['pool']['token0Price'])
          ]]), axis=0)
        return 1
      else:
        print(block_number, resp.status)
        return 0
  except asyncio.TimeoutError:
    if errors_count % 10000 == 0:
      print(f"{errors_count:,}", "errors")
    errors_count += 1
    return 0

async def fetch_concurrent(p):
  loop = asyncio.get_event_loop()
  async with aiohttp.ClientSession() as session:
    tasks = []
    for number in range(p):
      tasks.append(loop.create_task(fetch(session, number)))
    await asyncio.gather(*tasks)

asyncio.run(fetch_concurrent(plus))

pandas_data = pd.DataFrame(arr, columns=[
    'Block',
    json_data['data']['pool']['token0']['symbol'],
    json_data['data']['pool']['token1']['symbol'],
    'Transactions',
    'Price'
]).sort_values(by=['Block'], ignore_index=True)

pandas_data = pandas_data.astype({'Block': 'int', 'Transactions': 'int'})

pandas_data.to_csv(file_path, mode='a', index=False, header=not os.path.exists(file_path))
