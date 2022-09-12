import datetime
import typing


def get_new_objects(client, bucket: str,
                    folder_path: str,
                    last_modified: datetime.datetime) -> typing.List[typing.Tuple[str, datetime]]:
    """
    Retorna lista de novos objetos criados no bucket e na pasta especificada a partir de uma data.

    :param client: Client S3
    :param bucket: Bucket S3
    :param folder_path: Path da pasta no bucket
    :param last_modified: Data de filtro
    :return: Lista de tuplas com nome do objeto e data de modificação
    """

    utc_tz = datetime.timezone.utc  # alias

    response = client.list_objects_v2(
        Bucket=bucket,
        StartAfter=folder_path
    )

    objects: typing.List[typing.Tuple[str, datetime]] = []

    for obj in response['Contents']:
        modified_date = obj['LastModified']
        if modified_date > last_modified.replace(tzinfo=utc_tz):
            objects.append((obj['Key'], modified_date))

    return objects
