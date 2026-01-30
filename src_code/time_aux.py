from datetime import datetime, timezone


def dt2ts(dt):
    """
    Convert a timezone-aware UTC datetime to a Unix timestamp in milliseconds. Sub-milisecond precision is truncated.

    Parameters
    ----------
    dt : datetime.datetime
        Timezone-aware datetime object (UTC) representing the event time.

    Returns
    -------
    int
        Unix timestamp in milliseconds since the epoch (UTC).
    """
    return int(dt.timestamp() * 1000)


def ts2dt(ts):
    """
    Convert a Unix timestamp in milliseconds to a timezone-aware UTC datetime.

    Parameters
    ----------
    ts : int
        Unix timestamp in milliseconds since the epoch (UTC).

    Returns
    -------
    datetime.datetime
        Timezone-aware datetime object in UTC corresponding to the input
        timestamp.
    """
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)


def ensure_utc(dt):
    """
    Parameters
    ----------
    dt : datetime.datetime
        Timezone-aware datetime object (UTC) representing the event time.

    Returns
    -------
    datetime.datetime
        Timezone-aware datetime object in UTC corresponding to the input
        timestamp.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
