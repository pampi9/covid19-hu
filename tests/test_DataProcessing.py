import unittest

import model as md


class MyTestCase(unittest.TestCase):
    def test_correct_date(self):
        date = {
            "badly_formatted": {"input": "2020.11.10. 08.50.12", "output": "2020.11.10. 08:50:12"},
            "rightly_formatted": {"input": "2020.11.10. 08:50:12", "output": "2020.11.10. 08:50:12"}
        }
        for (key, value) in date.items():
            date_reformatted = md.Preprocessing.correct_date(value["input"])
            self.assertEqual(value["output"], date_reformatted)

    def test_check_consolidated_filename(self):
        filenames = {
            "correct": {"input": "2020.10.json", "output": True},
            "bad": {"input": "2020.11.22. 09:20.json", "output": False}
        }
        for (key, value) in filenames.items():
            check = md.Preprocessing.valid_consolidated(value["input"])
            self.assertEqual(value["output"], check)

    def test_load_existing(self):
        data_dir = "../data"
        my_process = md.Preprocessing()
        my_process.load_existing_data(data_dir=data_dir)
        self.assertEqual(2, len(my_process.df.keys()))
        self.assertTrue("kpi_hu" in my_process.df.keys())
        self.assertTrue("kpi_world" in my_process.df.keys())
        expected_columns = [
            'deaths', 'infected', 'recovered',
            'deaths_pest', 'infected_pest', 'recovered_pest',
            'deaths_other', 'infected_other', 'recovered_other',
            'lockeddown', 'tests', 'source', 'update', 'Country/Region'
        ]
        expected_columns.sort()
        received_columns = list(my_process.df["kpi_hu"].columns)
        received_columns.sort()
        self.assertEqual(expected_columns, received_columns)


if __name__ == '__main__':
    unittest.main()
