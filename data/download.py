import requests
from rich.progress import track
from rich.console import Console
import zipfile
import io
import asyncio
import aiohttp
import base64
from datetime import datetime

console = Console()
links = requests.get('https://www.opentopodata.org/datasets/aster30m_urls.txt').text.split('\n')
current, total = 0, len(links) 
saved = 0

header = {'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6IngxdjQiLCJleHAiOjE3MzIzMDk3MzgsImlhdCI6MTcyNzEyNTczOCwiaXNzIjoiaHR0cHM6Ly91cnMuZWFydGhkYXRhLm5hc2EuZ292In0.NTpULG-oyjqms43jjMdoo_1b4bmXadsJ0IGBzKy0NWiI1GGcoQz5_G_hQxlfb_5UswAerv_Zp7rL7W1Jo2fd3yfbGLlKm3GirIxGhchhatF7kECQaxKhQIHYaQHBTWLMY8Hr6te5w1c9mJcp0RYX_n3J48U231Hmb_z0Z4jBGWoZeJa4ybpXq6XalAFwIXKsgZ9QbB6M-I_VYIVUIkysE2lzvxHdczvrsYh4QjGS-JL6nxe5rkmFo_jzoYY7XZfmUsJAUqWaJlqdjPoaJt_PpqwAwLnDJ0YPHovNs-f7QRhKdJpnpJgUAw0uUDIWNcBjMewvGb1C8ytShtcrVPZqPw'}
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
    console.clear()
    console.print(start_time)
    current += 1
    console.print(f"\nNow [cyan]{current}[/cyan] out of [cyan]{total}[/cyan]")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(download_and_extract_zip(i, '/Users/Rober/dev/work/x_keeper/topography/opentopodata/data/aster30m'))
    # loop.run_until_complete(download_and_extract_zip(i, '/root/opentopodata/data/aster30m'))
    # console.clear()
end_time = datetime.now()
console.print(f'[yellow] took {end_time - start_time}')
console.print(f'[green]done![/green]\nMissing {total - saved} archives')