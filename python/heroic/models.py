import struct


class RowKey(object):
    TS = struct.Struct("!q")

    def __init__(self, key, timestamp, tags, buggy_tags):
        self.key = key
        self.timestamp = timestamp
        self.tags = tags
        self.buggy_tags = buggy_tags

    @classmethod
    def decode_tags(cls, bb):
        result = dict()
        data = bb.decode("utf-8")

        while len(data) > 0:
            index = data.find("=")
            end = data.find(":")

            if index == -1 or end == -1:
                break

            key = data[:index]
            value = data[index + 1:end]
            result[key] = value
            data = data[end + 1:]

        return result, len(data) > 0

    @classmethod
    def deserialize(cls, bb):
        index = bb.find("\0")
        key = bb[:index].decode("utf-8")
        (timestamp,) = cls.TS.unpack(bb[index + 1:index + 1 + cls.TS.size])
        tags, buggy_tags = cls.decode_tags(bb[index + 1 + cls.TS.size:])
        return cls(key, timestamp, tags, buggy_tags)

    def is_buggy(self):
        """
        Criterias for when this row key should be considered buggy.

        This is highly specific to the Heroic use-case.
        """

        # Buggy tag detected at de-serialization time.
        if self.buggy_tags:
            return True

        # These used to contributed to houndreds of thousands of timeseries.
        # Anything related to them should be purged.
        if "fantomextranscoder" in self.key:
            return True

        for key, value in self.tags.items():
            # Check if the structure of the tag indicates buginess.
            if self.buggy_tag(key, value):
                return True

            # Internal statistics about faulty metrics.
            # These are considered harmful if they are part of a deprecated
            # set.
            if self.key == "kairosdb.metric_counters":
                if key == "metric_name" and "fantomextranscoder" in value:
                    return True

            if self.key == "kairosdb.protocol.http_request_count":
                if key == "method" and "fantomextranscoder" in value:
                    return True

        return False

    def buggy_tag(self, key, value):
        if ":" in key or "=" in key:
            return True

        if ":" in value or "=" in value:
            return True

        return False

    def __repr__(self):
        tags = ", ".join("{}={}".format(*e) for e in
                         sorted(self.tags.items()))

        return (
            "RowKey(key={} timestamp={}, "
            "tags=({}), buggy_tags={}, is_buggy={})"
        ).format(self.key, self.timestamp, tags, self.buggy_tags,
                 self.is_buggy())


class DAO(object):
    def __init__(self, session):
        self.session = session

    def list_keys(self):
        last = ""

        for i in itertools.count():
            if last is None:
                break

            start = i * ns.limit
            stop = (i + 1) * ns.limit

            log.debug("Scanning from: {} - {}".format(start, stop))

            stmt = session.prepare(SELECT_STMT).bind((last, ns.limit))

            result = session.execute(stmt, timeout=None)

            last = None

            for row in result:
                key = RowKey.deserialize(row.key)
                log.info(repr(key))
                last = row.key
