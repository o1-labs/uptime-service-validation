from multiprocessing_copy import get_validate_state_hash
import unittest
import json

class TestingZkValidator(unittest.TestCase):
    def test_get_validate_state_hash(self):
        file_list = ['2023-10-26T00_01_06Z-B62qoWzouroXrwjqeV8RxHHRULUaxxeN1fh247EFz2R7uGJerAF4TSn.json',
                     '2023-10-30T00_00_51Z-B62qizdUTPz2GLKR3wYaV8brLWmeuktJWetouo3Lr1HqtXDStphj4fW.json']
        combine_list=list()
        get_validate_state_hash(file_list,combine_list)
        expected_1 = '{"state_hash": "3NKivyQ6kfvvhMPpKspyXQz6a8AiM3xTvJb8eHpAFqkpqKBAinse", "parent": "3NKMDRV23EQU1biiEGrQxFvkSUBhrHnHmss8U3EQ4A78Wnuo8R5X", "height": 299805, "slot": 457436}'
        expected_2 = '{"state_hash": "None", "height": 0, "slot": 0, "parent": "None"}'
        dumped_object_1 = json.dumps(combine_list[0][0])
        dumped_object_2 = json.dumps(combine_list[0][1])
        self.assertEqual(len(combine_list[0]), 2)
        self.assertEqual(dumped_object_1, expected_1)
        self.assertEqual(dumped_object_2, expected_2)

if __name__ == '__main__':
    unittest.main()