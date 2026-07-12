"""Shared constants for the asset-failure monitoring package."""

from typing import cast
from datetime import tzinfo

import pytz

UTC_TZ = cast(tzinfo, pytz.UTC)
