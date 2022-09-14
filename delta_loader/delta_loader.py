import datetime
import hashlib
from typing import Optional

import boto3
import typer
from delta import DeltaTable
from pyspark.sql import SparkSession

from cloud_utils.aws import dynamo, s3
from cloud_utils.databricks.databricks import is_running_on_databricks

from dotenv import load_dotenv


def get_job_watermark(client, job_id: str) -> str:
    item = dynamo.get_item(client, "job_watermarks", {"job_type": "delta-loader", "job_name": job_id})
    if item is None:
        return ""
    return item["last_folder"]


def put_job_watermark(client, job_id: str, last_folder: str) -> None:
    dynamo.put_item(client, "job_watermarks", {"job_type": "delta-loader", "job_name": job_id, "last_folder": last_folder})


def write_to_delta(spark: SparkSession,
                   staging_path: str,
                   table_path: str,
                   primary_key: str,
                   staging_format: str = "parquet",
                   write_type="merge"
                   ):
    staging_df = spark.read.format(staging_format).load(staging_path)

    if not DeltaTable.isDeltaTable(spark, table_path):
        staging_df.write.format('delta').save(table_path)

    elif write_type in ("overwrite", "append"):
        staging_df.write.format('delta').mode(write_type).save(table_path)

    elif write_type == "merge":
        delta_table = DeltaTable.forPath(spark, table_path)
        delta_table.alias("target").merge(
            staging_df.alias("source"),
            f"target.{primary_key} = source.{primary_key}"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    else:
        raise typer.BadParameter(f"Invalid write_type: {write_type}")

    # Update manifest file
    deltaTable = DeltaTable.forPath(spark, table_path)
    deltaTable.generate("symlink_format_manifest")


def get_spark_session(access_key, secret_key) -> SparkSession:
    if is_running_on_databricks():
        return SparkSession.builder.getOrCreate()
    else:
        return SparkSession.builder \
            .appName("delta-loader") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0,org.apache.hadoop:hadoop-aws:3.3.1") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.access.key", access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
            .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", True) \
            .getOrCreate()


def main(staging_bucket: str,
         staging_path: str,
         table_bucket: str,
         table_path: str,
         staging_format="parquet",
         write_type: str = "merge",
         primary_key: Optional[str] = None,
         aws_access_key_id: str = typer.Argument(None, envvar="AWS_ACCESS_KEY_ID"),
         aws_secret_access_key: str = typer.Argument(None, envvar="AWS_SECRET_ACCESS_KEY")
         ):
    """
    Carrega dados do staging para tabela delta

    :param staging_path: Path do arquivo delta no S3
    :param table_path: Path da tabela no DynamoDB
    :return: None
    """
    if write_type == "merge" and primary_key is None:
        raise typer.BadParameter("primary_key is required when write_type is merge")

    job_id = hashlib.md5((staging_path + "|" + table_path).encode()).hexdigest()

    aws_session: boto3.Session = boto3.session.Session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    spark = get_spark_session(aws_access_key_id, aws_secret_access_key)
    spark.sparkContext.setLogLevel('WARN')

    dynamo_client = aws_session.resource("dynamodb", region_name="us-east-1")
    s3_client = aws_session.resource("s3")

    typer.echo(f"Staging path: {staging_path}")
    typer.echo(f"Table path: {table_path}")
    typer.echo(f"Job ID: {job_id}")

    # Get watermark on dynamodb table based on job id
    last_folder = get_job_watermark(dynamo_client, job_id)
    typer.echo(f"Last Staging Folder: {last_folder}")

    typer.echo("Checking if there is new data to load...")
    folders = s3.get_subfolders_from_prefix(s3_client.meta.client, bucket=staging_bucket, prefix=staging_path)

    for folder in sorted(folders):
        if folder > last_folder:
            typer.echo(f"Loading data from {folder}")
            write_to_delta(spark, f"s3a://{staging_bucket}/{folder}", f"s3a://{table_bucket}/{table_path}", write_type=write_type, staging_format=staging_format, primary_key="id")
            put_job_watermark(dynamo_client, job_id, folder)


if __name__ == "__main__":
    typer.run(main)
