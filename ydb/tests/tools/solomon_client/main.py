#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import datetime
import os
import requests
from retry import retry_call
import urllib

timeout = 15
max_tries = 5
retry_delay = 2


def _do_request_inner(method, url, json, headers):
    resp = requests.request(method=method, url=url, timeout=timeout, json=json, headers=headers)
    resp.raise_for_status()
    return resp


def _do_request(method, url, json=None, headers=None):
    return retry_call(_do_request_inner, fkwargs={"method": method, "url": url, "json": json, "headers": headers}, tries=max_tries, delay=2)


def list_metrics(args):
    url = "https://{}/api/v2/projects/{}/sensors?".format(args.http_location, args.project)

    headers = {
        "Authorization": "OAuth y1__xC7u8WRpdT-ARjjECDh18kCKi0Pq8C25q2-TBK_sJVPRosAC5g",
        "accept": "application/json;charset=UTF-8"
    }

    request_params = {
        "projectId": args.project,
        "selectors": args.selectors,
        "forceCluster": "sas",
        "from": args.from_range,
        "to": args.to_range,
        "page": 0,
        "pageSize": 10000
    }

    pages_count = 1
    current_page = 0

    metrics = []

    while (current_page < pages_count):
        request_params["page"] = current_page

        response = _do_request(method="GET", url=url + urllib.parse.urlencode(request_params), json=None, headers=headers)
        response_data = response.json()

        for metric in response_data["result"]:
            metrics.append(metric)

        pages_count = response_data["page"]["pagesCount"]
        current_page += 1

    return metrics


def parse_args():
    program_name = 'solomon_client'
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description="solomon client util"
    )
    subparsers = parser.add_subparsers(help='sub-command help', required=True)
    list_parser = subparsers.add_parser(
        'list',
        formatter_class=argparse.RawTextHelpFormatter,
        description="""list metrics for specified selectors"""
    )
    list_parser.set_defaults(command=list_metrics)

    parser.add_argument("--http-location", type=str, required=False, default="solomon.yandex.net", help="Solomon installation http endpoint")
    parser.add_argument("--grpc-location", type=str, required=False, default="solomon.yandex.net", help="Solomon installation grpc endpoint")
    parser.add_argument("-P", "--project", type=str, required=True, help="Selectors project")
    parser.add_argument("-S", "--selectors", type=str, required=True, help="Selectors query")
    parser.add_argument("-F", "--from-range", type=str, required=False, default=(datetime.datetime.now() - datetime.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"), help="Left time range border")
    parser.add_argument("-T", "--to-range", type=str, required=False, default=datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"), help="Right time range border")
    return parser.parse_args()


def main():
    args = parse_args()
    args.command(args)
