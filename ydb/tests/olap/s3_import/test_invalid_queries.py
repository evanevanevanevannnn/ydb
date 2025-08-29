import logging
import pytest

from ydb.tests.olap.common.s3_client import S3Mock, S3Client
from ydb.tests.olap.s3_import.base import S3ImportTestBase

logger = logging.getLogger(__name__)


class TestInvalidQueries(S3ImportTestBase):
    def test_check_invalid_credentials(self):
        self.ydb_client.query("""
            CREATE TABLE olap_table (
                key Int32 NOT NULL,
                data String,
                PRIMARY KEY (key)
            ) WITH (
                STORE = COLUMN
            );
        """)

        self.ydb_client.query("""
            UPSERT INTO olap_table (key, data) VALUES (0, "test data");
        """)

        test_bucket = "test_bucket"
        self.s3_client.create_bucket(test_bucket)

        access_key_id_secret_name = f"{test_bucket}_key_id"
        access_key_secret_secret_name = f"{test_bucket}_key_secret"
        self.ydb_client.query(f"CREATE OBJECT {access_key_id_secret_name} (TYPE SECRET) WITH value='invalid id'")
        self.ydb_client.query(f"CREATE OBJECT {access_key_secret_secret_name} (TYPE SECRET) WITH value='invalid secret'")

        self.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE s3_source WITH (
                SOURCE_TYPE = "ObjectStorage",
                LOCATION = "{self.s3_mock.endpoint}/{test_bucket}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_id_secret_name}",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="{access_key_secret_secret_name}",
                AWS_REGION="{self.s3_client.region}"
            );

            CREATE EXTERNAL TABLE s3_table (
                key Int32 NOT NULL,
                data String
            ) WITH (
                DATA_SOURCE="s3_source",
                LOCATION="/test_folder/",
                FORMAT="parquet"
            );
        """)

        logger.info("Exporting into s3...")

        self.ydb_client.query(f"INSERT INTO s3_table SELECT * FROM olap_table")
        logger.info(f"Exporting finished, bucket stats: {self.s3_client.get_bucket_stat(test_bucket)}")

        logger.info("Importing into ydb...")
        self.ydb_client.query(f"""
            CREATE TABLE from_s3 (
                PRIMARY KEY (key)
            ) WITH (
                STORE = COLUMN
            ) AS SELECT * FROM s3_table
        """)
