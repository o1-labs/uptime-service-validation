from datetime import datetime, timedelta
from helper import pullFileNames, getTimeBatches
import unittest

class TestingHelpers(unittest.TestCase):
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

    def testGetTmeBatches(self):
        a = datetime(2023, 11, 6, 15, 35, 47, 630499)
        b = a + timedelta(minutes=5)
        result = getTimeBatches(a,b,10)
        self.assertEqual(len(result), 10)
        self.assertEqual(result[0], (a, datetime(2023, 11, 6, 15, 36, 17, 630499)))
        self.assertEqual(result[1], (datetime(2023, 11, 6, 15, 36, 17, 630499), datetime(2023, 11, 6, 15, 36, 47, 630499)))
        self.assertEqual(result[2], (datetime(2023, 11, 6, 15, 36, 47, 630499), datetime(2023, 11, 6, 15, 37, 17, 630499)))
        self.assertEqual(result[3], (datetime(2023, 11, 6, 15, 37, 17, 630499), datetime(2023, 11, 6, 15, 37, 47, 630499)))
        self.assertEqual(result[4], (datetime(2023, 11, 6, 15, 37, 47, 630499), datetime(2023, 11, 6, 15, 38, 17, 630499)))
        self.assertEqual(result[5], (datetime(2023, 11, 6, 15, 38, 17, 630499), datetime(2023, 11, 6, 15, 38, 47, 630499)))
        self.assertEqual(result[6], (datetime(2023, 11, 6, 15, 38, 47, 630499), datetime(2023, 11, 6, 15, 39, 17, 630499)))
        self.assertEqual(result[7], (datetime(2023, 11, 6, 15, 39, 17, 630499), datetime(2023, 11, 6, 15, 39, 47, 630499)))
        self.assertEqual(result[8], (datetime(2023, 11, 6, 15, 39, 47, 630499), datetime(2023, 11, 6, 15, 40, 17, 630499)))
        self.assertEqual(result[9], (datetime(2023, 11, 6, 15, 40, 17, 630499),b))

if __name__ == '__main__':
    unittest.main()