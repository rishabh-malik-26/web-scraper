import asyncio
import aiohttp
import pandas as pd 
import time



df = pd.read_csv(r"C:\Users\Rishabh\Downloads\ML_Datasets\Phishing_Urls\url_dataset.csv")

all_urls =  list(df['url'])


## Function to divide data into chunks 
def chunkify(data,batch_size):
    for i in range(0,len(data),batch_size):
        batch = data[i:i+batch_size]
        yield batch


## Fuction to fetch url's status code

async def fetch(url,session):
        try:
            async with session.get(url,timeout = 2) as response:
                return response.status
        except Exception as e:
            return None


## Fuction to get status codes of all urls 
async def main(urls):
    results = []
    connector = aiohttp.TCPConnector(limit= 1000)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch(url,session) for url in urls]
        results = await asyncio.gather(*tasks,return_exceptions=True)
    return results


batch = 5000


start = time.perf_counter()


for index, a in enumerate(chunkify(all_urls,batch)):  
    status = asyncio.run(main(a))
    new_df = pd.DataFrame(status)

    new_df.to_csv('output.csv', mode='a', header=not pd.io.common.file_exists('output.csv'), index=False)
    print(f"Batch {index +1} completed")

end = time.perf_counter()

print(f"Time taken:{end - start}")



