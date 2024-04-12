from math import pi
from area.area_determ import AreaDeterminant


def test_circle_simple_1():
    assert pi == AreaDeterminant.get_area(1)


def test_circle_simple_2():
    assert 0 == AreaDeterminant.get_area(0)


def test_circle_non_valid_1():
    try:
        AreaDeterminant.get_area(-1)
        assert False
    except Exception as e:
        assert True
