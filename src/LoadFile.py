import requests
from tqdm import tqdm
import os
from urllib.parse import urlsplit


class LoadFile:
    def __init__(self, url: str) -> None:
        self.url = url
        urlPath = urlsplit(url).path
        fileName: str = os.path.basename(urlPath)
        self.targetFile: str = os.path.join(os.getcwd(), 'download', fileName)
        self.blockSize: int = 1024
    

    def startLoading(self) -> str:
        response = requests.get(self.url, stream=True)

        totalSizeInBytes: int = int(response.headers.get('content-length', 0))

        progressBar = tqdm(total=totalSizeInBytes, unit='iB', unit_scale=True)
        with open(self.targetFile, 'wb') as f:
            for data in response.iter_content(self.blockSize):
                progressBar.update(len(data))
                f.write(data)
        progressBar.close()

        if totalSizeInBytes != 0 and progressBar.n != totalSizeInBytes:
            raise Exception("Error: Download incomplete.")
        
        print("File downloaded successfully.")
        return self.targetFile