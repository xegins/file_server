import os
import sys
import json

import typing as tp

from hashlib import md5
from pathlib import Path

from aiohttp import web


routes = web.RouteTableDef()


def load_settings(config_path: Path) -> tp.Dict[str, tp.Any]:
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config file with path: '{config_path}' - not found!"
        )
    with config_path.open() as config_file:
        return json.load(config_file)


@routes.get('/')
async def root_page(request: web.Request) -> web.Response:
    return web.json_response(
        {
            "description": "Hi!, it's information how to use this API.",
            "upload": "For upload file send POST request to path '/file'.",
            "download": "For download file send GET request to path '/file/{name}'",
            "delete": "For removing file send DELETE request to path '/file', "
                      "request type must be 'json' and contain key 'name'."
        }
    )


@routes.post('/file')
async def upload_file(request: web.Request) -> web.Response:
    body: bytes = await request.read()
    file_hash: str = md5(body).hexdigest()

    file_dir: Path = Path(os.getcwd(), f"store/{file_hash[:2]}/")
    if not file_dir.exists():
        file_dir.mkdir(parents=True)

    with open(Path(file_dir, file_hash), "wb") as f:
        f.write(body)

    return web.json_response(
        {
            "file_hash": file_hash
        }
    )


@routes.get('/file/{name}')
async def download_file(request: web.Request) -> tp.Union[web.FileResponse, web.Response]:
    file_hash: str = request.match_info["name"]
    file_path: Path = Path(os.getcwd(), f"store/{file_hash[:2]}/{file_hash}")
    if not file_path.exists():
        return web.json_response(
            {
                "description": f"File {file_hash} - not found!"
            },
            status=web.HTTPBadRequest.status_code
        )
    return web.FileResponse(file_path)


@routes.delete('/file')
async def delete_file(request: web.Request) -> web.Response:
    if request.content_type != "application/json":
        return web.json_response(
            {
                "description": "Content type must be 'json'!"
            },
            status=web.HTTPBadRequest.status_code
        )

    body: tp.Dict = await request.json()
    if "name" not in body:
        return web.json_response(
            {
                "description": "Key 'name' must be in request!"
            },
            status=web.HTTPBadRequest.status_code
        )

    file_hash: str = body["name"]
    file_path: Path = Path(os.getcwd(), f"store/{file_hash[:2]}/{file_hash}")
    if not file_path.exists():
        return web.json_response(
            {
                "description": f"File {file_hash} - not found!"
            },
            status=web.HTTPBadRequest.status_code
        )

    file_path.unlink()
    # Удаляем директорию, если в ней больше нет файлов
    if not any(file_path.parent.iterdir()):
        file_path.parent.rmdir()

    return web.json_response(
        {
            "description": f"File '{file_hash}' - was removed!"
        }
    )


if __name__ == '__main__':
    config_file_path: str = sys.argv[1] if len(sys.argv) == 2 else "config.json"
    settings: tp.Dict = load_settings(Path(config_file_path))

    app: web.Application = web.Application()
    app.add_routes(routes)
    web.run_app(app, **settings)
