from aiohttp import web
from datetime import datetime, timezone
import json
import logging
import re
import time
import threading
from concurrent import futures

from library.python.monlib.encoder import loads
from .multi_shard import MultiShard
from .shard import Shard

import grpc
from ydb.library.yql.providers.solomon.solomon_accessor.grpc.data_service_pb2_grpc import \
    DataServiceServicer, add_DataServiceServicer_to_server
from ydb.library.yql.providers.solomon.solomon_accessor.grpc.data_service_pb2 import ReadRequest, ReadResponse, MetricType

routes = web.RouteTableDef()
logger = logging.getLogger(__name__)

CONTENT_TYPE_SPACK = "application/x-solomon-spack"
CONTENT_TYPE_JSON = "application/json"


def _parse_selectors(selectors):
    result = dict()

    match = re.search(r".*{(.*)}.*", selectors)
    if (not match):
        return (result, False)

    group = match[1]
    if (len(group) == 0):
        return (result, True)

    for selector in group.split(","):
        eq_pos = selector.find("=")
        if (eq_pos == -1):
            return (result, False)

        key = selector[:eq_pos].strip()
        value = selector[eq_pos + 1:].strip('=').strip().strip('"')
        result[key] = value

    return (result, True)


def _parse_iso_to_ms(iso_str):
    """Parse an ISO 8601 timestamp string to milliseconds since epoch.

    Handles formats produced by TInstant::ToString(), e.g.:
      "2026-06-16T09:00:00Z"
      "2026-06-16T09:00:00.000000Z"
    """
    s = iso_str.strip()
    # Remove trailing 'Z' and parse as UTC
    if s.endswith('Z'):
        s = s[:-1]
    # Try parsing with fractional seconds first, then without
    for fmt in ('%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S'):
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse timestamp: {iso_str}")


class SolomonEmulator(object):
    def __init__(self, config):
        self._config = config
        self._api_calls = 0
        self._data = MultiShard()

        # gRPC concurrency tracking (thread-safe, gRPC runs in a thread pool)
        self._grpc_lock = threading.Lock()
        self._grpc_concurrent_reads = 0
        self._grpc_max_concurrent_reads = 0
        self._grpc_total_reads = 0
        self._grpc_read_delay_sec = 0.0

    def on_read_start(self):
        with self._grpc_lock:
            self._grpc_concurrent_reads += 1
            self._grpc_total_reads += 1
            if self._grpc_concurrent_reads > self._grpc_max_concurrent_reads:
                self._grpc_max_concurrent_reads = self._grpc_concurrent_reads

    def on_read_end(self):
        with self._grpc_lock:
            self._grpc_concurrent_reads -= 1

    def get_grpc_stats(self):
        with self._grpc_lock:
            return {
                "max_concurrent_reads": self._grpc_max_concurrent_reads,
                "total_reads": self._grpc_total_reads,
            }

    def reset_grpc_stats(self):
        with self._grpc_lock:
            self._grpc_concurrent_reads = 0
            self._grpc_max_concurrent_reads = 0
            self._grpc_total_reads = 0

    def set_grpc_read_delay(self, delay_sec):
        with self._grpc_lock:
            self._grpc_read_delay_sec = delay_sec

    def get_grpc_read_delay(self):
        with self._grpc_lock:
            return self._grpc_read_delay_sec

    def _get_shard(self, project, cluster, service):
        return self._data.get_or_create(project, cluster, service)

    def _handle_auth(self, request):
        if self._config.auth:
            auth_header = request.headers.get('AUTHORIZATION')
            if not self._config.auth == auth_header:
                logger.debug(f"Authorization header {auth_header} mismatches expected value {self._config.auth}")
                raise web.HTTPForbidden()

    async def get_ping(self, request):
        return web.Response(status=200)

    async def api_v2_push(self, request):
        self._api_calls += 1

        logger.debug("push: {}".format(await request.read()))
        self._handle_auth(request)

        project = request.rel_url.query['project']
        cluster = request.rel_url.query['cluster']
        service = request.rel_url.query['service']

        shard = self._get_shard(project, cluster, service)
        content_type = request.headers['content-type']

        if content_type == CONTENT_TYPE_SPACK:
            metrics_json = json.loads(loads(await request.read()))
            logger.debug(f"spack decoded: {metrics_json}")
        elif content_type == CONTENT_TYPE_JSON:
            metrics_json = await request.json()
            logger.debug(f"json received: {metrics_json}")
        else:
            return web.HTTPBadRequest(text=f"Unknown content type {content_type}")

        return web.json_response({"sensorsProcessed": shard.add_metrics(metrics_json)})

    async def data_write(self, request):
        self._api_calls += 1

        logger.debug("write: {}".format(await request.read()))
        self._handle_auth(request)

        folder_id = request.rel_url.query['folderId']
        service = request.rel_url.query['service']

        shard = self._get_shard(folder_id, folder_id, service)
        content_type = request.headers['content-type']

        if content_type != CONTENT_TYPE_JSON:
            return web.HTTPBadRequest(text=f"Unknown content type {content_type}")

        metrics_json = await request.json()

        return web.json_response({"writtenMetricsCount": shard.add_metrics(metrics_json)})

    async def sensor_names(self, request):
        self._api_calls += 1

        json = await request.json()
        selectors, success = _parse_selectors(json["selectors"])

        if not success:
            return web.HTTPBadRequest(text="Invalid selectors")

        if "project" not in selectors or "cluster" not in selectors or "service" not in selectors:
            return web.HTTPBadRequest(text="project, cluster and service labels must be specified")

        if "projectId" in json:
            return web.HTTPBadRequest(text="Invalid query params")

        project = selectors["project"]
        cluster = selectors["cluster"]
        service = selectors["service"]

        shard = self._get_shard(project, cluster, service)
        result = shard.get_label_names(selectors)

        return web.json_response({"names": result})

    async def sensor_labels(self, request):
        self._api_calls += 1

        json = await request.json()
        selectors, success = _parse_selectors(json["selectors"])

        if not success:
            return web.HTTPBadRequest(text="Invalid selectors")

        if "project" not in selectors or "cluster" not in selectors or "service" not in selectors:
            return web.HTTPBadRequest(text="project, cluster and service labels must be specified")

        if "projectId" in json or "pageSize" in json:
            return web.HTTPBadRequest(text="Invalid query params")

        project = selectors["project"]
        cluster = selectors["cluster"]
        service = selectors["service"]

        shard = self._get_shard(project, cluster, service)
        labels, totalCount = shard.get_labels(selectors)

        return web.json_response({"labels": labels, "totalCount": totalCount})

    async def sensors(self, request):
        self._api_calls += 1

        json = await request.json()
        selectors, success = _parse_selectors(json["selectors"])

        if not success:
            return web.HTTPBadRequest(text="Invalid selectors")

        if "project" not in selectors or "cluster" not in selectors or "service" not in selectors:
            return web.HTTPBadRequest(text="project, cluster and service labels must be specified")
        if "projectId" in json:
            return web.HTTPBadRequest(text="Invalid query params")

        project = selectors["project"]
        cluster = selectors["cluster"]
        service = selectors["service"]

        shard = self._get_shard(project, cluster, service)
        metrics, error = shard.get_metrics(selectors)

        if error is not None:
            return web.HTTPBadRequest(text=error)

        return web.json_response({"result": metrics, "page": {"pagesCount": 1, "totalCount": len(metrics)}})

    async def sensors_data(self, request):
        """Handle GetPointsCount requests: POST /api/v2/projects/{project}/sensors/data"""
        self._api_calls += 1

        project = request.match_info["project"]
        body = await request.json()

        program = body.get("program", "")
        from_str = body.get("from", "")
        to_str = body.get("to", "")

        # Parse timestamps from ISO 8601 strings (e.g. "2026-06-16T09:00:00.000000Z")
        try:
            from_ms = _parse_iso_to_ms(from_str)
            to_ms = _parse_iso_to_ms(to_str)
        except Exception as e:
            return web.HTTPBadRequest(text=f"Invalid timestamp: {e}")

        # Extract selectors from count({selectors}) program
        inner = program
        if inner.startswith("count(") and inner.endswith(")"):
            inner = inner[6:-1]

        selectors, success = _parse_selectors(inner)
        if not success:
            return web.HTTPBadRequest(text="Invalid selectors in program")

        # Determine project/cluster/service from selectors
        sel_project = selectors.get("project", project)
        sel_cluster = selectors.get("cluster", sel_project)
        sel_service = selectors.get("service", "")

        if not sel_service:
            return web.HTTPBadRequest(text="service label must be specified")

        shard = self._get_shard(sel_project, sel_cluster, sel_service)
        count = shard.get_points_count(selectors, from_ms, to_ms)

        return web.json_response({"scalar": count})

    async def metrics_get(self, request):
        cluster = request.rel_url.query.get('cluster', None) or request.rel_url.query['folderId']
        project = request.rel_url.query.get('project', cluster)
        service = request.rel_url.query['service']

        shard = self._get_shard(project, cluster, service)
        if shard is None:
            return web.HTTPNotFound(text=f"Unable to find shard {project}/{cluster}/{service}")
        reply = shard.as_text()
        return web.json_response(text=reply)

    async def metrics_post(self, request):
        project = request.rel_url.query['project']
        cluster = request.rel_url.query['cluster']
        service = request.rel_url.query['service']

        metrics_json = json.loads(await request.read())

        shard = self._get_shard(project, cluster, service)
        shard.add_parsed_metrics(metrics_json)

        return web.Response(status=200)

    async def get_api_calls(self, request):
        return web.json_response({"api_calls": self._api_calls})

    async def cleanup(self, request):
        cluster = request.rel_url.query.get('cluster', None) or request.rel_url.query.get('folderId', None)
        project = request.rel_url.query.get('project', cluster)
        service = request.rel_url.query.get('service', None)

        if project is None and cluster is None and service is None:
            self._data.clear()
        else:
            self._data.delete(project, cluster, service)
        return web.Response(status=200)

    async def cleanup_api_calls(self, request):
        self._api_calls = 0
        return web.Response(status=200)

    async def grpc_stats(self, request):
        return web.json_response(self.get_grpc_stats())

    async def grpc_stats_reset(self, request):
        self.reset_grpc_stats()
        return web.Response(status=200)

    async def grpc_config(self, request):
        delay = float(request.rel_url.query.get('read_delay_sec', '0'))
        self.set_grpc_read_delay(delay)
        return web.Response(status=200)

    def inc_api_calls(self):
        self._api_calls += 1


class DataService(DataServiceServicer):
    def __init__(self, emulator):
        self._emulator = emulator

    def Read(self, request: ReadRequest, context) -> ReadResponse:
        logger.debug('ReadRequest: %s', request)

        self._emulator.inc_api_calls()
        self._emulator.on_read_start()
        try:
            delay = self._emulator.get_grpc_read_delay()
            if delay > 0:
                time.sleep(delay)

            if request.container.HasField("project_id") and request.container.project_id in Shard.DEPRECATED_TESTS_PROJECTS:
                return self.DeprecatedTestsLogic(request, context)

            response = ReadResponse()

            for query in request.queries:
                selectors, success = _parse_selectors(str(query.value))

                if not success:
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details("Coulnd't parse selectors")
                    return ReadResponse()

                if request.container.HasField("project_id"):
                    selectors["project"] = request.container.project_id
                else:
                    del selectors["folderId"]
                    selectors["project"] = request.container.folder_id
                    selectors["cluster"] = request.container.folder_id

                if "project" not in selectors or "cluster" not in selectors or "service" not in selectors:
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details("Selectors should contain ['project', 'cluster', 'service'] labels")
                    return ReadResponse()

                project = selectors["project"]
                cluster = selectors["cluster"]
                service = selectors["service"]

                shard = self._emulator._get_shard(project, cluster, service)
                result, error = shard.get_data(selectors, request.from_time, request.to_time, request.downsampling)

                if len(error):
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details(error)
                    return ReadResponse()

                response_query = response.response_per_query.add()
                response_query.query_name = "query"

                timeseries = response_query.timeseries_vector.values.add()
                for key, value in result["labels"].items():
                    timeseries.labels[key] = str(value)
                timeseries.type = DataService._map_metric_type(result["type"])

                timeseries.timestamp_values.values.extend(result["timestamps"])
                timeseries.double_values.values.extend(result["values"])

            logger.debug('ReadResponse: %s', response)
            return response
        finally:
            self._emulator.on_read_end()

    def DeprecatedTestsLogic(self, request: ReadRequest, context):
        project = request.container.project_id
        if project == "invalid":
            logger.debug("invalid project_id, sending error")
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"Project {project} does not exist")
            return ReadResponse()

        if project == "my_project" or project == "hist":
            labels = self._dict_to_labels(request)
            labels["project"] = project

            response = ReadResponse()

            response_query = response.response_per_query.add()
            response_query.query_name = "query"

            timeseries = response_query.timeseries_vector.values.add()
            for key, value in labels.items():
                timeseries.labels[key] = str(value)
            timeseries.type = DataService._map_metric_type("RATE")

            timeseries.timestamp_values.values.extend([10000, 20000, 30000])
            timeseries.double_values.values.extend([100, 200, 300])

            return response

    @staticmethod
    def _map_metric_type(kind):
        if (kind == "DGAUGE"):
            return MetricType.DGAUGE
        elif (kind == "IGAUGE"):
            return MetricType.IGAUGE
        elif (kind == "COUNTER"):
            return MetricType.COUNTER
        else:
            return MetricType.RATE

    @staticmethod
    def _dict_to_labels(request):
        result = dict()

        result["from"] = str(request.from_time)
        result["to"] = str(request.to_time)
        result["program"] = f"program length {len(str(request.queries[0].value))}"
        if request.downsampling.HasField("disabled"):
            result["downsampling.disabled"] = f"bool {True}"
        else:
            result["downsampling.aggregation"] = request.downsampling.grid_aggregation
            result["downsampling.fill"] = request.downsampling.gap_filling
            result["downsampling.gridMillis"] = f"int {request.downsampling.grid_interval}"
            result["downsampling.disabled"] = f"bool {False}"

        return result


def create_web_app(emulator):
    webapp = web.Application()
    webapp.add_routes([
        web.post("/api/v2/projects/{project}/sensors/data", emulator.sensors_data),
        web.post("/api/v2/projects/{project}/sensors/names", emulator.sensor_names),
        web.post("/api/v2/projects/{project}/sensors/labels", emulator.sensor_labels),
        web.post("/api/v2/projects/{project}/sensors", emulator.sensors),
        web.get("/api/calls", emulator.get_api_calls),
        web.get("/metrics/get", emulator.metrics_get),
        web.get("/ping", emulator.get_ping),
        web.post("/api/v2/push", emulator.api_v2_push),
        web.post("/monitoring/v2/data/write", emulator.data_write),
        web.post("/metrics/post", emulator.metrics_post),
        web.post("/cleanup", emulator.cleanup),
        web.post("/cleanup/api/calls", emulator.cleanup_api_calls),
        web.get("/grpc/stats", emulator.grpc_stats),
        web.post("/grpc/stats/reset", emulator.grpc_stats_reset),
        web.post("/grpc/config", emulator.grpc_config)
    ])

    return webapp


def create_grpc_server(emulator, port):
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    add_DataServiceServicer_to_server(
        DataService(emulator), grpc_server
    )
    grpc_server.add_insecure_port(f'[::]:{port}')

    return grpc_server


def run_web_app(config, http_port, grpc_port):
    emulator = SolomonEmulator(config)

    app = create_web_app(emulator)
    server = create_grpc_server(emulator, grpc_port)

    server.start()
    web.run_app(app, port=http_port)
