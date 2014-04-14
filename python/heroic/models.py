import struct


class RowKey(object):
    TS = struct.Struct("!q")

    def __init__(self, raw, key, timestamp, tags, buggy_tags):
        self.raw = raw
        self.key = key
        self.timestamp = timestamp
        self.tags = tags
        self.buggy_tags = buggy_tags

    @classmethod
    def decode_tags(cls, raw):
        result = dict()
        data = raw.decode("utf-8")

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
    def deserialize(cls, raw):
        index = raw.find("\0")
        key = raw[:index].decode("utf-8")
        (timestamp,) = cls.TS.unpack(raw[index + 1:index + 1 + cls.TS.size])
        tags, buggy_tags = cls.decode_tags(raw[index + 1 + cls.TS.size:])
        return cls(raw, key, timestamp, tags, buggy_tags)

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


