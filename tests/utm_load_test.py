from utm_load import get_date_string, create_session
import pytest, requests
from datetime import datetime

@pytest.fixture
def ukg_session():
    return create_session()



@pytest.mark.parametrize(
    ('date', 'datestring'),
        [(datetime(2025,5,1), "2025-05-01"),
        (datetime(2024,12,10),  "2024-12-10")])
def test_get_date_string(date, datestring):
    assert get_date_string(date) == datestring
def test_create_session(ukg_session):
    assert ukg_session.__class__ == requests.Session
    assert ukg_session.auth is not None