# -*- coding: utf-8 -*-
"""glutils.job_utils

This module contains commonly used functions for the glue etl jobs.
"""
import base64
import zlib


def zipped_b64_to_string(val):
    if val:
        zipped_string = base64.b64decode(val)
        return zlib.decompress(zipped_string, 16+zlib.MAX_WBITS).decode('utf-8')
