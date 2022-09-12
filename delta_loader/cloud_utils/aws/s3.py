import datetime
import typing


def get_new_objects(client, bucket: str,
                    folder_path: str,
                    last_modified: datetime.datetime) -> typing.Tuple[typing.List[str], datetime.datetime]:
    utc_tz = datetime.timezone.utc  # alias

    response = client.list_objects_v2(
        Bucket=bucket,
        StartAfter=folder_path
    )

    objects = []
    max_modified_date = datetime.datetime(1970, 1, 1, tzinfo=utc_tz)

    for obj in response['Contents']:
        modified_date = obj['LastModified']
        if modified_date > last_modified.replace(tzinfo=utc_tz):
            objects.append(obj['Key'])

        max_modified_date = max(max_modified_date, modified_date)

    return objects, max_modified_date
