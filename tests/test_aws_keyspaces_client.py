from datetime import datetime
from uptime_service_validation.coordinator.aws_keyspaces_client import (
    AWSKeyspacesClient,
    ShardCalculator,
)


def test_get_submitted_at_date_list():
    start = datetime(2023, 11, 6, 15, 35, 47, 630499)
    end = datetime(2023, 11, 6, 15, 45, 47, 630499)
    result = AWSKeyspacesClient.get_submitted_at_date_list(start, end)
    assert result == ["2023-11-06"]

    start = datetime(2023, 11, 6, 15, 35, 47, 630499)
    end = datetime(2023, 11, 7, 11, 45, 47)
    result = AWSKeyspacesClient.get_submitted_at_date_list(start, end)
    assert result == ["2023-11-06", "2023-11-07"]

    start = datetime(2023, 11, 6, 15, 35, 47, 630499)
    end = datetime(2023, 11, 8, 0, 0, 0)
    result = AWSKeyspacesClient.get_submitted_at_date_list(start, end)
    assert result == ["2023-11-06", "2023-11-07", "2023-11-08"]


def test_calculate_shard():
    matrix = [
        [0, 0, 0, 0],
        [0, 0, 1, 0],
        [0, 2, 59, 1],
        [12, 1, 3, 300],
        [15, 10, 0, 379],
        [23, 22, 23, 584],
        [23, 59, 59, 599],
    ]
    for i in matrix:
        hour = i[0]
        minute = i[1]
        second = i[2]
        expected = i[3]
        assert ShardCalculator.calculate_shard(hour, minute, second) == expected


def test_calculate_shards_in_range():
    # 1 between days
    start_time = datetime(2024, 2, 29, 23, 58, 29)
    end_time = datetime(2024, 3, 1, 0, 3, 29)
    expected_cql_statement = "shard in (0,1,599)"
    result_cql_statement = ShardCalculator.calculate_shards_in_range(
        start_time, end_time
    )
    assert result_cql_statement == expected_cql_statement

    # 2 within the same day
    start_time = datetime(2024, 2, 29, 12, 58, 1)
    end_time = datetime(2024, 2, 29, 12, 59, 59)
    expected_cql_statement = "shard in (324)"
    result_cql_statement = ShardCalculator.calculate_shards_in_range(
        start_time, end_time
    )
    assert result_cql_statement == expected_cql_statement

    # 2 shard boundary
    start_time = datetime(2024, 2, 29, 0, 0, 0)
    end_time = datetime(2024, 2, 29, 0, 2, 24)
    expected_cql_statement = "shard in (0,1)"
    result_cql_statement = ShardCalculator.calculate_shards_in_range(
        start_time, end_time
    )
    assert result_cql_statement == expected_cql_statement
