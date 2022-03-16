import uuid
import pytest


@pytest.fixture
def test_id():
    eid = uuid.uuid4().__str__()
    return eid.replace('-', '')
