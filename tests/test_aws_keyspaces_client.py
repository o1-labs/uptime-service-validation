from datetime import datetime
from uptime_service_validation.coordinator.aws_keyspaces_client import (
    AWSKeyspacesClient,
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
