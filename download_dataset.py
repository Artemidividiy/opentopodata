import requests
from rich.progress import track
from rich.console import Console
import zipfile
import io
import asyncio
import aiohttp
import base64
from datetime import datetime
import os

console = Console()
links = requests.get('https://www.opentopodata.org/datasets/aster30m_urls.txt').text.split('\n')
current, total = 0, len(links) 
saved = 0
dir_to_save_to = os.getenv('DATA_DIRECTORY')

russian_critical_points = {
    'northernmost': {
        'longitude': 81.5035,
        'latitude': 59.1422
    },
    'southernmost': {
        'longitude': 41.1314,
        'latitude': 47.5128
    },
    'westernmost': {
        'longitude': 57.2730,
        'latitude': 19.3819
    },
    'easternmost': {
        'longitude': 65.47,
        'latitude': 169.01
    },
}

if dir_to_save_to is None: 
    dir_to_save_to = '/Users/Rober/dev/work/x_keeper/topography/opentopodata/data/aster30m'

header = {'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6IngxdjQiLCJleHAiOjE3MzIzMDk3MzgsImlhdCI6MTcyNzEyNTczOCwiaXNzIjoiaHR0cHM6Ly91cnMuZWFydGhkYXRhLm5hc2EuZ292In0.NTpULG-oyjqms43jjMdoo_1b4bmXadsJ0IGBzKy0NWiI1GGcoQz5_G_hQxlfb_5UswAerv_Zp7rL7W1Jo2fd3yfbGLlKm3GirIxGhchhatF7kECQaxKhQIHYaQHBTWLMY8Hr6te5w1c9mJcp0RYX_n3J48U231Hmb_z0Z4jBGWoZeJa4ybpXq6XalAFwIXKsgZ9QbB6M-I_VYIVUIkysE2lzvxHdczvrsYh4QjGS-JL6nxe5rkmFo_jzoYY7XZfmUsJAUqWaJlqdjPoaJt_PpqwAwLnDJ0YPHovNs-f7QRhKdJpnpJgUAw0uUDIWNcBjMewvGb1C8ytShtcrVPZqPw'}

def check_file_exists(link_to_file_to_download: str): 
    check_dir_exists(dir_to_save_to)
    file_name_to_download = link_to_file_to_download.split('/')[-1].split('.')[0]
    file_names = [file for file in os.listdir(dir_to_save_to) if os.path.isfile(os.path.join(dir_to_save_to, file))]
    for i in file_names: 
        if os.path.isfile(os.path.join(dir_to_save_to, i)) and i.split('.')[0][:-4:] == file_name_to_download:
            return True
    return False

def check_if_in_russia(file_name): 
    coordinates_in_file = file_name.split('_')[1].split('.')[0]
    if coordinates_in_file[0] == 'S': return False
    if int("".join(coordinates_in_file[1:3])) > russian_critical_points['northernmost']['longitude']: return False
    if int("".join(coordinates_in_file[1:3])) < russian_critical_points['southernmost']['longitude']: return False
    if coordinates_in_file[4] == 'W' and int("".join(coordinates_in_file[5:7])) < russian_critical_points['easternmost']['latitude']: return False
    if coordinates_in_file[4] == 'E' and int("".join(coordinates_in_file[5:7])) > russian_critical_points['westernmost']['latitude']: return False
    
    return True

def check_dir_exists(dir_name): 
    if not os.path.exists(dir_name): 
        os.makedirs(os.path.join(dir_to_save_to))
    return 

async def download_and_extract_zip(url, destination_folder):
    async with aiohttp.ClientSession(headers=header) as session:
        # auth = aiohttp.BasicAuth(login='x1v4', password='Artemiy1234;')
        async with session.get(url) as response:
            console.log(response)
            if response.status == 200:
                console.print(f'{url} [green]status: 200[/green]')
                zip_data = await response.read()
                with zipfile.ZipFile(io.BytesIO(zip_data), 'r') as zip_ref:
                    zip_ref.extractall(destination_folder, [file for file in zip_ref.filelist if '_num' not in file.filename])
            else:
                console.print(f"Failed to download the zip archive: {response.status}")
start_time = datetime.now()
for i in track(links, 'downloading dataset'):
    if check_file_exists(i): continue
    if not check_if_in_russia(i.split('/')[-1]):
        console.print(f"[red]{i.split('/')[-1]}[/red] not in Russia")
        continue
    console.print(start_time)
    current += 1
    console.print(f"\nNow [cyan]{current}[/cyan] out of [cyan]{total}[/cyan]")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(download_and_extract_zip(i, dir_to_save_to))
    # loop.run_until_complete(download_and_extract_zip(i, '/root/opentopodata/data/aster30m'))
    
end_time = datetime.now()
console.print(f'[yellow] took {end_time - start_time}')
console.print(f'[green]done![/green]\nMissing {total - saved} archives')