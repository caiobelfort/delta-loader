import datetime
import typing
from typing import List, Any


def get_subfolders_from_prefix(client,
                               bucket: str,
                               prefix: str) -> typing.List[str]:
    """
    Retorna lista de novos objetos criados no bucket e na pasta especificada a partir de uma data.

    :param client: Client S3
    :param bucket: Bucket S3
    :param folder_path: Path da pasta no bucket
    :param last_modified: Data de filtro
    :return: Lista de tuplas com nome do objeto e data de modificação
    """

    paginator = client.get_paginator('list_objects_v2')
    response = paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
        Delimiter="/"
    )

    result: list[Any] = []
    for page in response:
        files = page['CommonPrefixes']
        result.extend([f['Prefix'] for f in files])

    return result
