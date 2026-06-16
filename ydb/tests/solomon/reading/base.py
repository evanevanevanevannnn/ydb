# -*- coding: utf-8 -*-
import os
import time
from datetime import datetime, timezone

from ydb.library.yql.tools.solomon_emulator.client.client import cleanup_emulator, add_solomon_metrics

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels

import ydb
from ydb.issues import GenericError


class SolomonReadingTestBase(object):
    @classmethod
    def setup_class(cls, test_name):
        cleanup_emulator()

        config = KikimrConfigGenerator(
            extra_feature_flags={"enable_external_data_sources": True},
            additional_log_configs={'KQP_COMPUTE': LogLevels.TRACE},
        )
        config.yaml_config["query_service_config"] = {}
        config.yaml_config["query_service_config"]["available_external_data_sources"] = ["Solomon"]
        config.yaml_config["query_service_config"]["solomon"] = {
            "default_settings": [
                {
                    "name": "_EnableReading",
                    "value": "true"
                },
                {
                    "name": "_EnableRuntimeListing",
                    "value": "true"
                },
                {
                    "name": "_EnableSolomonClientPostApi",
                    "value": "true"
                },
                {
                    "name": "_MaxListingPageSize",
                    "value": 1000
                },
                {
                    "name": "MaxApiInflight",
                    "value": 2500
                }
            ]
        }

        if test_name == "settings_validation":
            add_solomon_metrics("settings_validation", "settings_validation", "my_service", {"metrics": [
                {
                    "labels"        : {"test_type": "setting_validation"},
                    "type"          : "DGAUGE",
                    "timestamps"    : [1000000],
                    "values"        : [0]
                }
            ]})

        elif test_name == "basic_reading":
            # Generate 12 timestamps at 5-second intervals, base aligned to 15s
            base_sec, end_sec, timestamps = cls._generate_recent_timestamps(
                count=12, interval_sec=5, align_sec=15, offset_sec=120
            )
            cls.basic_reading_from_sec = base_sec
            cls.basic_reading_to_sec = end_sec
            cls.basic_reading_from_iso = cls._ts_to_iso(base_sec)
            cls.basic_reading_to_iso = cls._ts_to_iso(end_sec)
            cls.basic_reading_timestamps = timestamps
            cls.basic_reading_values = list(range(12))

            add_solomon_metrics("basic_reading", "basic_reading", "my_service", {"metrics": [
                {
                    "labels"        : {"test_type": "basic_reading_test"},
                    "type"          : "DGAUGE",
                    "timestamps"    : cls.basic_reading_timestamps,
                    "values"        : cls.basic_reading_values
                }
            ]})

        elif test_name == "listing_paging":
            cls.listing_paging_metrics_size = 2500

            # Listing uses a single recent timestamp per metric.
            # Align to 15s grid so points survive default downsampling filter.
            now_sec = cls._now_epoch_sec()
            now_sec = now_sec - (now_sec % 15)
            cls.listing_paging_from_iso = cls._ts_to_iso(now_sec - 120)
            cls.listing_paging_to_iso = cls._ts_to_iso(now_sec + 60)

            add_solomon_metrics("listing_paging", "listing_paging", "my_service", {"metrics": [
                *cls._generate_listing_paging_test_metrics(cls.listing_paging_metrics_size, now_sec)
            ]})

        elif test_name == "listing_batching":
            cls.listing_batching_metrics_sizes = [1100, 600]

            # Align to 15s grid so points survive default downsampling filter.
            now_sec = cls._now_epoch_sec()
            now_sec = now_sec - (now_sec % 15)
            cls.listing_batching_from_iso = cls._ts_to_iso(now_sec - 120)
            cls.listing_batching_to_iso = cls._ts_to_iso(now_sec + 60)

            add_solomon_metrics("listing_batching", "listing_batching", "my_service", {"metrics": [
                *cls._generate_listing_batching_test_metrics(*cls.listing_batching_metrics_sizes, now_sec)
            ]})

        elif test_name == "data_paging":
            cls.data_paging_timeseries_size = 2500
            # Generate 2500 timestamps at 1-second intervals, ~50 min ago
            base_sec, end_sec, timestamps = cls._generate_recent_timestamps(
                count=2500, interval_sec=1, align_sec=1, offset_sec=3000
            )
            cls.data_paging_from_iso = cls._ts_to_iso(base_sec)
            cls.data_paging_to_iso = cls._ts_to_iso(end_sec)
            cls.data_paging_timestamps = timestamps
            cls.data_paging_values = list(range(2500))

            config.yaml_config["query_service_config"]["solomon"]["default_settings"].extend([
                {
                    "name": "MaxPointsPerOneRequest",
                    "value": 1000
                }
            ])

            add_solomon_metrics("data_paging", "data_paging", "my_service", {"metrics": [
                {
                    "labels"        : {"test_type": "data_paging_test"},
                    "type"          : "DGAUGE",
                    "timestamps"    : cls.data_paging_timestamps,
                    "values"        : cls.data_paging_values
                }
            ]})

        elif test_name == "backpressure_test":
            cls.backpressure_test_metrics_size = 100

            config.yaml_config["query_service_config"]["solomon"]["default_settings"].extend([
                {
                    "name": "MaxDataInflightMb",
                    "value": 1
                },
                {
                    "name": "MetricsQueuePrefetchSize",
                    "value": 1
                },
                {
                    "name": "MetricsQueueBatchCountLimit",
                    "value": 1
                },
                {
                    "name": "ComputeActorBatchSize",
                    "value": 1
                },
                {
                    "name": "MaxSelectorsPerBatch",
                    "value": 1
                },
                {
                    "name": "MaxApiInflight",
                    "value": 3
                }
            ])

            # Align to 15s grid so points survive default downsampling filter.
            now_sec = cls._now_epoch_sec()
            now_sec = now_sec - (now_sec % 15)
            cls.backpressure_from_iso = cls._ts_to_iso(now_sec - 120)
            cls.backpressure_to_iso = cls._ts_to_iso(now_sec + 60)

            add_solomon_metrics("backpressure_test", "backpressure_test", "my_service", {"metrics": [
                *cls._generate_backpressure_test_metrics(cls.backpressure_test_metrics_size, now_sec)
            ]})

        elif test_name == "data_batching":
            cls.data_batching_metrics_size = 10

            config.yaml_config["query_service_config"]["solomon"]["default_settings"].extend([
                {
                    "name": "MaxSelectorsPerBatch",
                    "value": 5
                },
                {
                    "name": "MaxPointsPerOneRequest",
                    "value": 1000
                }
            ])

            # Align to 15s grid so points survive default downsampling filter.
            now_sec = cls._now_epoch_sec()
            now_sec = now_sec - (now_sec % 15)
            cls.data_batching_from_iso = cls._ts_to_iso(now_sec - 120)
            cls.data_batching_to_iso = cls._ts_to_iso(now_sec + 60)

            add_solomon_metrics("data_batching", "data_batching", "my_service", {"metrics": [
                *cls._generate_data_batching_metrics(cls.data_batching_metrics_size, now_sec)
            ]})

        cls.solomon_http_endpoint = os.environ.get("SOLOMON_HTTP_ENDPOINT")
        cls.solomon_grpc_endpoint = os.environ.get("SOLOMON_GRPC_ENDPOINT")

        cls.cluster = KiKiMR(config)
        cls.cluster.start()

        cls.endpoint = "%s:%s" % (
            cls.cluster.nodes[1].host, cls.cluster.nodes[1].port
        )
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database='/Root',
                endpoint=cls.endpoint
            )
        )
        cls.driver.wait()

    @classmethod
    def teardown_class(cls):
        cls.driver.stop()
        cls.cluster.stop()

    @classmethod
    def execute_query(cls, query):
        with ydb.QuerySessionPool(cls.driver) as session_pool:
            try:
                res = session_pool.execute_with_retries(query)
                return (res, None)
            except GenericError as generic_error:
                return (None, generic_error)

    # ── Timestamp helpers ─────────────────────────────────────────────────

    @staticmethod
    def _now_epoch_sec():
        """Current time truncated to whole seconds."""
        return int(time.time())

    @staticmethod
    def _ts_to_iso(epoch_sec):
        """Convert epoch seconds to ISO 8601 UTC string for SQL queries."""
        return datetime.fromtimestamp(epoch_sec, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    @staticmethod
    def _generate_recent_timestamps(count, interval_sec, align_sec=1, offset_sec=120):
        """Generate timestamps relative to now.

        Args:
            count: number of timestamps to generate
            interval_sec: spacing between consecutive timestamps (seconds)
            align_sec: align base timestamp to this grid (for clean downsampling tests)
            offset_sec: how far back from now to start (seconds)

        Returns:
            (base_sec, end_sec, timestamps) where timestamps are epoch seconds
        """
        now = int(time.time())
        base_sec = now - offset_sec
        # Align base to grid for clean downsampling tests
        if align_sec > 1:
            base_sec = base_sec - (base_sec % align_sec)
        timestamps = [base_sec + i * interval_sec for i in range(count)]
        end_sec = timestamps[-1] + interval_sec  # exclusive upper bound
        return base_sec, end_sec, timestamps

    # ── Test data generators ──────────────────────────────────────────────

    @staticmethod
    def _generate_listing_paging_test_metrics(size, ts_sec):
        listing_paging_metrics = [
            {
                "labels"        : {"test_type": "listing_paging_test", "test_label": str(i)},
                "type"          : "DGAUGE",
                "timestamps"    : [ts_sec],
                "values"        : [0]
            }
            for i in range(size)
        ]

        listing_paging_metrics.append({
            "labels"        : {"test_type": "listing_paging_test"},
            "type"          : "DGAUGE",
            "timestamps"    : [ts_sec],
            "values"        : [0]
        })

        return listing_paging_metrics

    @staticmethod
    def _generate_listing_batching_test_metrics(totalSize, firstLabelSize, ts_sec):
        listing_batching_metrics = [
            {
                "labels"        : {"test_type": "listing_batching_test", "test_label": str(i)},
                "type"          : "DGAUGE",
                "timestamps"    : [ts_sec],
                "values"        : [0]
            }
            for i in range(firstLabelSize)
        ]
        for i in range(totalSize - firstLabelSize):
            listing_batching_metrics.append({
                "labels"        : {"test_type": "listing_batching_test", "test_label": "0", "test_label_2": str(i)},
                "type"          : "DGAUGE",
                "timestamps"    : [ts_sec],
                "values"        : [0]
            })

        return listing_batching_metrics

    @staticmethod
    def _generate_data_paging_timeseries(size):
        timestamps = [i * 300 for i in range(size)]
        values = [i for i in range(size)]
        return timestamps, values

    @staticmethod
    def _generate_backpressure_test_metrics(size, ts_sec):
        backpressure_test_metrics = [
            {
                "labels"        : {"test_type": "backpressure_test", "test_label": str(i)},
                "type"          : "DGAUGE",
                "timestamps"    : [ts_sec],
                "values"        : [0]
            }
            for i in range(size)
        ]

        return backpressure_test_metrics

    @staticmethod
    def _generate_data_batching_metrics(size, ts_sec):
        return [
            {
                "labels"        : {"test_type": "data_batching_test", "test_label": str(i)},
                "type"          : "DGAUGE",
                "timestamps"    : [ts_sec],
                "values"        : [i]
            }
            for i in range(size)
        ]
