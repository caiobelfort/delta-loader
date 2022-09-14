from typing import Optional


def get_item(client, table_name: str, key: dict) -> Optional[dict]:
    """
    Retorna item da tabela DynamoDB.

    :param client: Client DynamoDB
    :param table_name: Nome da tabela
    :param key: Chave do item
    :return: Item
    """

    table = client.Table(table_name)
    response = table.get_item(
        TableName=table_name,
        Key=key
    )

    if "Item" in response:
        return response["Item"]
    return None


def put_item(client, table_name: str, item: dict) -> None:
    """
    Insere item na tabela DynamoDB.

    :param client: Client DynamoDB
    :param table_name: Nome da tabela
    :param item: Item
    :return: None
    """
    table = client.Table(table_name)
    table.put_item(
        TableName=table_name,
        Item=item
    )
