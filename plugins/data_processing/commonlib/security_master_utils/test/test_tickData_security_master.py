import os, sys
import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import identifier_utils as tdm


CUSIP = (
    ("013817101000", "US0138171014"),
    ("13817309", "US0138173093"),
    ("037833100", "US0378331005"),
    ("30303M102", "US30303M1027"),
    ("78462F103001", "US78462F1030")
)


ISIN = (
    ("US0378331005", "037833100"),
    ("US30303M1027", "30303M102")
)

@pytest.mark.parametrize("cusip_code, expected", CUSIP)
def test_cusip_to_isin(cusip_code, expected):
    actual = tdm.convert_cusip_to_isin(cusip_code)
    assert actual == expected




@pytest.mark.parametrize("isin_code, expected", ISIN)
def test_isin_to_cusip(isin_code, expected):
    actual = tdm.convert_isin_to_cusip(isin_code)
    assert actual == expected
