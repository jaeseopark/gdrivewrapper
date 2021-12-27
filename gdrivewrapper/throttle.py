from googleapiclient.http import DEFAULT_CHUNK_SIZE

MIN_CHUNK_SIZE = 10 * pow(2, 20)  # 10 MB
MAX_CHUNK_SIZE = 100 * pow(2, 20)  # 100 MB


def get_chunk_size(max_speed: int) -> int:
    if max_speed is None:
        # use Google SDK's default value
        return DEFAULT_CHUNK_SIZE

    suggested = int(max_speed / 5)

    if suggested > MAX_CHUNK_SIZE:
        return MAX_CHUNK_SIZE

    if suggested < MIN_CHUNK_SIZE:
        return MIN_CHUNK_SIZE

    return suggested
