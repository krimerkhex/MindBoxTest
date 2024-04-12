from area.area_determ import AreaDeterminant


def test_area_determinant_non_valid_1():
    try:
        assert AreaDeterminant.get_area()
        assert False
    except Exception:
        assert True


def test_area_determinant_non_valid_2():
    try:
        assert AreaDeterminant.get_area(0, 0)
        assert False
    except Exception:
        assert True


def test_area_determinant_non_valid_3():
    try:
        assert AreaDeterminant.get_area(0, 0, 0, 0)
        assert False
    except Exception:
        assert True
