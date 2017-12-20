# -*- coding: utf-8 -*-
"""glutils.job_utils

This module contains commonly used functions for the glue etl jobs.
"""
import base64
import json
import zlib
import socket
import struct


def zipped_b64_to_string(val):  # pylint:disable=inconsistent-return-statements
    if val:
        zipped_string = base64.b64decode(val)
        return zlib.decompress(zipped_string, 16 + zlib.MAX_WBITS).decode('utf-8')


def code_format(val):  # pylint:disable=inconsistent-return-statements
    if val:
        return val.strip().replace(" ", "_").upper()


# UDF to read DynamoDB Json values directly
def get_dynamodb_value(dynamodb_string):  # pylint:disable=inconsistent-return-statements
    if dynamodb_string:
        return list(json.loads(dynamodb_string).values())[0]


# UDF to convert IPv4 int to standard ip address
def ip_address(ip):  # pylint:disable=inconsistent-return-statements
    if ip:
        ip_int = int(ip)
        return socket.inet_ntoa(struct.pack('!L', ip_int))
