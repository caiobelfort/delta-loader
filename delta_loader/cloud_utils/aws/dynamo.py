def get_item(client, table_name: str, key: dict) -> dict:
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

    return response['Item']
