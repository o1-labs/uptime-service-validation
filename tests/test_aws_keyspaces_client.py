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
        [0, 0, 0],
        [0, 1, 0],
        [0, 59, 9],
        [1, 3, 10],
        [10, 0, 100],
        [22, 23, 223],
        [44, 45, 447],
        [59, 54, 599],
        [59, 59, 599],
    ]
    for i in matrix:
        minute = i[0]
        second = i[1]
        expected = i[2]
        assert ShardCalculator.calculate_shard(minute, second) == expected


def test_calculate_shards_in_range():
    # 1 between days
    start_time = datetime(2024, 2, 29, 23, 58, 29)
    end_time = datetime(2024, 3, 1, 0, 3, 29)
    expected_cql_statement = "shard in (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,584,585,586,587,588,589,590,591,592,593,594,595,596,597,598,599)"
    result_cql_statement = ShardCalculator.calculate_shards_in_range(
        start_time, end_time
    )
    assert result_cql_statement == expected_cql_statement

    # 2 within the same day
    start_time = datetime(2024, 2, 29, 12, 58, 1)
    end_time = datetime(2024, 2, 29, 12, 59, 59)
    expected_cql_statement = "shard in (580,581,582,583,584,585,586,587,588,589,590,591,592,593,594,595,596,597,598,599)"
    result_cql_statement = ShardCalculator.calculate_shards_in_range(
        start_time, end_time
    )
    assert result_cql_statement == expected_cql_statement
