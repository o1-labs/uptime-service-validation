from datetime import datetime, timedelta
from helper import pullFileNames, getTimeBatches
import pytest

# The folloiwng two tests will fail as I have not given an accurate bucket name.
# def testFilePullSmallRange(self):
#     start_time = datetime.strptime('2023-08-03T16:31:58Z',"%Y-%m-%dT%H:%M:%SZ")
#     end_time = datetime.strptime('2023-08-03T16:31:59Z', "%Y-%m-%dT%H:%M:%SZ")
#     filtered_list = pullFileNames(start_time, end_time, "block-bucket-name", True)
#     self.assertEqual(len(filtered_list), 1)

# def testFilePullLargeRange(self):
#     start_time = datetime.strptime('2023-08-03T16:32:00Z',"%Y-%m-%dT%H:%M:%SZ")
#     end_time = datetime.strptime('2023-08-03T16:33:59Z', "%Y-%m-%dT%H:%M:%SZ")
#     filtered_list = pullFileNames(start_time, end_time, "block-bucket-name", True)
#     self.assertEqual(len(filtered_list), 11)


def test_get_time_batches():
    a = datetime(2023, 11, 6, 15, 35, 47, 630499)
    b = a + timedelta(minutes=5)
    result = getTimeBatches(a, b, 10)

    assert len(result) == 10
    assert result[0] == (a, datetime(2023, 11, 6, 15, 36, 17, 630499))
    assert result[1] == (
        datetime(2023, 11, 6, 15, 36, 17, 630499),
        datetime(2023, 11, 6, 15, 36, 47, 630499),
    )
    assert result[2] == (
        datetime(2023, 11, 6, 15, 36, 47, 630499),
        datetime(2023, 11, 6, 15, 37, 17, 630499),
    )
    assert result[3] == (
        datetime(2023, 11, 6, 15, 37, 17, 630499),
        datetime(2023, 11, 6, 15, 37, 47, 630499),
    )
    assert result[4] == (
        datetime(2023, 11, 6, 15, 37, 47, 630499),
        datetime(2023, 11, 6, 15, 38, 17, 630499),
    )
    assert result[5] == (
        datetime(2023, 11, 6, 15, 38, 17, 630499),
        datetime(2023, 11, 6, 15, 38, 47, 630499),
    )
    assert result[6] == (
        datetime(2023, 11, 6, 15, 38, 47, 630499),
        datetime(2023, 11, 6, 15, 39, 17, 630499),
    )
    assert result[7] == (
        datetime(2023, 11, 6, 15, 39, 17, 630499),
        datetime(2023, 11, 6, 15, 39, 47, 630499),
    )
    assert result[8] == (
        datetime(2023, 11, 6, 15, 39, 47, 630499),
        datetime(2023, 11, 6, 15, 40, 17, 630499),
    )
    assert result[9] == (datetime(2023, 11, 6, 15, 40, 17, 630499), b)
