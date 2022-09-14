import os


def is_running_on_databricks() -> bool:
    """
    Checa se o código está rodando no ambiente do databricks
    :return: True se estiver no Databricks False caso contrário
    """
    return True if os.environ.get('DATABRICKS_RUNTIME_VERSION') is not None else False
