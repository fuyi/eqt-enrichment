from unittest import TestCase
import pytest

from . import reformat_date


class TestStringToDate(TestCase):
    def test_empty_string(self):
        assert reformat_date('') is None

    def test_correct_input(self):
        assert reformat_date('Dec 2020') == '2020-12'

    def test_mal_formatted_input(self):
        with pytest.raises(KeyError):
            reformat_date('december 2020')
