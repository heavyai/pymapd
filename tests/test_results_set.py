from pymapd.cursor import make_row_results_set


class TestRowResults(object):

    def test_basic(self, rowwise):
        result = list(make_row_results_set(rowwise))
        expected = [
            ('2006-01-05', 'BUY', 'RHAT', 100, 35.13999938964844,
             1.100000023841858),
            ('2006-01-05', 'BUY', 'GOOG', 100, 12.140000343322754,
             1.2000000476837158)
        ]
        assert result == expected

    def test_basic_colwise(self, colwise):
        expected = [
            ('2006-01-05', 'BUY', 'RHAT', 100, 35.13999938964844,
             1.100000023841858),
            ('2006-01-05', 'BUY', 'GOOG', 100, 12.140000343322754,
             1.2000000476837158)
        ]

        result = list(make_row_results_set(colwise))
        assert result == expected
