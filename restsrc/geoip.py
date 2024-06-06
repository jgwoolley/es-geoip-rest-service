from typing import Optional
from time import time
from pydantic import BaseModel, Field, DirectoryPath

def get_updated_time():
    #TODO: Read from file metadata
    return int(time())

class GeoipDatabase(BaseModel):
    name: str
    url: str
    md5_hash: str
    age: Optional[str] = None
    provider: Optional[str] = None
    updated: int = Field(default_factory=get_updated_time)

class ApiConfig(BaseModel):
    database_path: str

from fastapi import FastAPI
from pydantic import BaseModel, Field, DirectoryPath
from typing import Optional, List
from fastapi.staticfiles import StaticFiles
from starlette.requests import Request
from os import listdir
from os.path import join as path_join
from os.path import isfile, isdir
from os import mkdir
from hashlib import md5
import tarfile

import logging

logger = logging.getLogger("geoip-rest")

def create_api(config:ApiConfig) -> FastAPI:
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)

    api = FastAPI()

    #TODO: Update name
    database_path = str(config.database_path)
    compressed_path = path_join(database_path, "tgz")
    if not isdir(compressed_path):
        mkdir(compressed_path)
    database_path = path_join(database_path, "mmdb")
    if not isdir(database_path):
        mkdir(database_path)    

    for input_name in listdir(database_path):
        if not input_name.endswith(".mmdb"):
            continue
        input_path = path_join(database_path, input_name)
        if not isfile(input_path):
            continue
        output_name = input_name.split(".")[0] + ".tgz"
        output_path = path_join(compressed_path, input_name)
        with tarfile.open(output_path, "w:gz") as tar:
            tar.add(input_path, output_name)
        
        logger.info(f"Compressed {input_path} into {output_path}", extra={
            "input_path": input_path,
            "output_path": output_path,
        })

    logger.info(f"Compressed all files in {database_path} into {compressed_path}", extra={
        "database_path": database_path,
        "compressed_path": compressed_path,
    })

    api.mount("/files", StaticFiles(directory=str(compressed_path)), name="files")

    @api.get("/")
    def list_databases(requests: Request) -> List[GeoipDatabase]:
        databases: List[GeoipDatabase] = []
        for file_name in listdir(compressed_path):
            file_path = path_join(compressed_path, file_name)
            if not isfile(file_path):
                continue
            url = f"{requests.base_url}files/{file_name}"
            with open(file_path, 'rb') as file:
                data = file.read()
                md5_hash = md5(data).hexdigest()
                database = GeoipDatabase(
                    name=file_name,
                    url=url,
                    md5_hash=md5_hash,
                )
                databases.append(database)

        print(databases)

        return databases

    return api

app = create_api(ApiConfig(database_path="."))