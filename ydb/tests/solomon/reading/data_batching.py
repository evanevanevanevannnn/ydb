import logging

from ydb.library.yql.tools.solomon_emulator.client.client import get_api_calls_count, cleanup_api_calls
from ydb.tests.library.test_meta import link_test_case

from .base import SolomonReadingTestBase

logger = logging.getLogger(__name__)


class TestDataBatching(SolomonReadingTestBase):
    @classmethod
    def setup_class(cls):
        super().setup_class("data_batching")

    def check_data_batching_result(self, result_set, error, expected_size):
        if error is not None:
            return False, error

        rows = []
        for result in result_set:
            rows.extend(result.rows)

        if len(rows) != expected_size:
            return False, "Result size differs from expected: have {}, should be {}".format(len(rows), expected_size)

        values = sorted([int(row["value"]) for row in rows])

        for i in range(expected_size):
            if values[i] != i:
                return False, "Missing value = {}".format(i)

        return True, None

    @link_test_case("#16396")
    def test_data_batching_solomon(self):
        data_source_query = f"""
            CREATE EXTERNAL DATA SOURCE local_solomon WITH (
                SOURCE_TYPE     = "Monium.Metrics",
                LOCATION        = "{self.solomon_http_endpoint}",
                GRPC_LOCATION   = "{self.solomon_grpc_endpoint}",
                AUTH_METHOD     = "NONE",
                USE_TLS         = "false"
            )"""
        result, error = self.execute_query(data_source_query)
        assert error is None

        # Read all 10 metrics with MaxSelectorsPerBatch=5 — should batch into
        # 2 gRPC calls per sub-range instead of 10 individual calls.
        query = f"""
            SELECT value FROM local_solomon.data_batching WITH (
                selectors = @@{{cluster="data_batching", service="my_service", test_type="data_batching_test"}}@@,

                from = "{self.data_batching_from_iso}",
                to = "{self.data_batching_to_iso}"
            )
        """
        cleanup_api_calls()

        success, error = self.check_data_batching_result(*self.execute_query(query), self.data_batching_metrics_size)
        assert success, error

        # With MaxSelectorsPerBatch=5 and 10 metrics, we expect significantly
        # fewer API calls than 10 (the unbatched case). The exact count depends
        # on the number of sub-ranges but must be strictly less than the metric
        # count, proving batching occurred.
        api_call_count = get_api_calls_count()
        assert api_call_count < self.data_batching_metrics_size, \
            "Expected fewer API calls than metrics due to batching, have {} calls for {} metrics".format(
                api_call_count, self.data_batching_metrics_size)

    @link_test_case("#23191")
    def test_data_batching_monitoring(self):
        data_source_query = f"""
            CREATE EXTERNAL DATA SOURCE local_monitoring WITH (
                SOURCE_TYPE     = "Monium.Metrics",
                LOCATION        = "{self.solomon_http_endpoint}",
                GRPC_LOCATION   = "{self.solomon_grpc_endpoint}",
                PROJECT         = "data_batching",
                CLUSTER         = "data_batching",
                AUTH_METHOD     = "NONE",
                USE_TLS         = "false"
            )"""
        result, error = self.execute_query(data_source_query)
        assert error is None

        # Same test as Solomon mode but using Monitoring-style external data source.
        query = f"""
            SELECT value FROM local_monitoring.my_service WITH (
                selectors = @@{{test_type="data_batching_test"}}@@,

                from = "{self.data_batching_from_iso}",
                to = "{self.data_batching_to_iso}"
            )
        """
        cleanup_api_calls()

        success, error = self.check_data_batching_result(*self.execute_query(query), self.data_batching_metrics_size)
        assert success, error

        api_call_count = get_api_calls_count()
        assert api_call_count < self.data_batching_metrics_size, \
            "Expected fewer API calls than metrics due to batching, have {} calls for {} metrics".format(
                api_call_count, self.data_batching_metrics_size)
