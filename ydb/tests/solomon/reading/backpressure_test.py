import logging

from ydb.library.yql.tools.solomon_emulator.client.client import (
    get_grpc_stats, configure_grpc_read_delay, reset_grpc_stats
)
from ydb.tests.library.test_meta import link_test_case

from .base import SolomonReadingTestBase

logger = logging.getLogger(__name__)


class TestBackpressure(SolomonReadingTestBase):
    # MaxApiInflight is set to 3 in base.py for backpressure_test.
    MAX_API_INFLIGHT = 3

    @classmethod
    def setup_class(cls):
        super().setup_class("backpressure_test")

    def check_backpressure_test_result(self, result_set, error, expected_size):
        if error is not None:
            return False, error

        rows = []
        for result in result_set:
            rows.extend(result.rows)

        if len(rows) != expected_size:
            return False, "Result size differs from expected: have {}, should be {}".format(len(rows), expected_size)

        return True, None

    @link_test_case("#16396")
    def test_backpressure_solomon(self):
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

        # Configure emulator with a read delay to create a concurrency window.
        # With 100 metrics, MaxSelectorsPerBatch=1, and 50ms delay per Read,
        # without backpressure all 100 requests would fire concurrently.
        configure_grpc_read_delay(0.05)
        reset_grpc_stats()

        query = f"""
            SELECT value FROM local_solomon.backpressure_test WITH (
                selectors = @@{{cluster="backpressure_test", service="my_service", test_type="backpressure_test"}}@@,

                from = "{self.backpressure_from_iso}",
                to = "{self.backpressure_to_iso}"
            )
        """

        success, error = self.check_backpressure_test_result(
            *self.execute_query(query), self.backpressure_test_metrics_size)
        assert success, error

        # Verify backpressure directly: the max concurrent gRPC reads must
        # not exceed MaxApiInflight (3).
        stats = get_grpc_stats()
        max_concurrent = stats["max_concurrent_reads"]
        total_reads = stats["total_reads"]

        assert total_reads > 0, "Expected at least one gRPC Read call, got 0"
        assert max_concurrent <= self.MAX_API_INFLIGHT, \
            "Backpressure violated: max concurrent reads {} exceeded MaxApiInflight {}".format(
                max_concurrent, self.MAX_API_INFLIGHT)

        # Clean up: remove read delay for subsequent tests
        configure_grpc_read_delay(0)

    @link_test_case("#23191")
    def test_backpressure_monitoring(self):
        data_source_query = f"""
            CREATE EXTERNAL DATA SOURCE local_monitoring WITH (
                SOURCE_TYPE     = "Monium.Metrics",
                LOCATION        = "{self.solomon_http_endpoint}",
                GRPC_LOCATION   = "{self.solomon_grpc_endpoint}",
                PROJECT         = "backpressure_test",
                CLUSTER         = "backpressure_test",
                AUTH_METHOD     = "NONE",
                USE_TLS         = "false"
            )"""
        result, error = self.execute_query(data_source_query)
        assert error is None

        configure_grpc_read_delay(0.05)
        reset_grpc_stats()

        query = f"""
            SELECT value FROM local_monitoring.my_service WITH (
                selectors = @@{{test_type="backpressure_test"}}@@,

                from = "{self.backpressure_from_iso}",
                to = "{self.backpressure_to_iso}"
            )
        """

        success, error = self.check_backpressure_test_result(
            *self.execute_query(query), self.backpressure_test_metrics_size)
        assert success, error

        stats = get_grpc_stats()
        max_concurrent = stats["max_concurrent_reads"]
        total_reads = stats["total_reads"]

        assert total_reads > 0, "Expected at least one gRPC Read call, got 0"
        assert max_concurrent <= self.MAX_API_INFLIGHT, \
            "Backpressure violated: max concurrent reads {} exceeded MaxApiInflight {}".format(
                max_concurrent, self.MAX_API_INFLIGHT)

        configure_grpc_read_delay(0)
