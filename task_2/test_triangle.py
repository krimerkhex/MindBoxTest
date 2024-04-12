from area.area_determ import AreaDeterminant


def test_triangle_1():
    assert 17 == round(AreaDeterminant.get_area(5, 7, 8.6))


def test_triangle_2():
    assert 6 == AreaDeterminant.get_area(4, 5, 3)


def test_triangle_3():
    assert 0 == AreaDeterminant.get_area(1, 2, 3)


def test_triangle_4():
    assert 4 == round(AreaDeterminant.get_area(3, 3, 3))


def test_triangle_non_valid_1():
    try:
        AreaDeterminant.get_area(10, 60, 30)
        assert False
    except Exception:
        assert True


def test_triangle_non_valid_2():
    try:
        AreaDeterminant.get_area(10, 60, -30)
        assert False
    except Exception:
        assert True
