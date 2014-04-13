import struct


class RowKey(object):
    TS = struct.Struct("!q")

    def __init__(self, key, timestamp, tags):
        self.key = key
        self.timestamp = timestamp
        self.tags = tags

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

        return result

    @classmethod
    def deserialize(cls, bb):
        index = bb.find("\0")
        key = bb[:index].decode("utf-8")
        (timestamp,) = cls.TS.unpack(bb[index + 1:index + 1 + cls.TS.size])
        tags = cls.decode_tags(bb[index + 1 + cls.TS.size:])
        return cls(key, timestamp, tags)

    def is_buggy(self):
        for key, value in self.tags.items():
            if ":" in key or "=" in key:
                return True

            if ":" in value or "=" in value:
                return True

            if key == "metric_name" and "fantomextranscoder" in value:
                return True

        return False

    def __repr__(self):
        return "{} {} {}".format(self.key, self.timestamp, repr(self.tags))
