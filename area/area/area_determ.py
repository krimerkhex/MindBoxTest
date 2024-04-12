from math import pi, sqrt
from .loger import Loger, logger


class AreaDeterminant:
    @classmethod
    @logger.catch
    def get_area(cls, *args: float) -> float | None:
        try:
            if cls._validation_data(*args):
                count = len(args)
                match count:
                    case 1:
                        return cls._circle_area(*args)
                    case 3:
                        return cls._triangle_area(*args)
                raise ValueError("No matching function for this arguments")
        except Exception as e:
            logger.exception(e)
        return None

    @staticmethod
    # @Loger
    def _validation_data(*args: float) -> bool:
        for value in args:
            if value is None or value < 0:
                return False
        if args[0] + args[1] < args[2] or args[2] + args[1] < args[0] or args[0] + args[2] < args[1]:
            raise ArithmeticError("Such a triangle cannot exist")
        return True

    @classmethod
    # @Loger
    def _determine_triangle_type(cls, *args: float) -> float:
        args = list(args)
        args.sort()
        if args[2] ** 2 == args[0] ** 2 + args[1] ** 2:
            area = cls._right_triangle_area(args[0], args[1])
        elif args[0] == args[1] == args[2]:
            area = cls._equilateral_triangle_area(args[0])
        else:
            area = cls._triangle_area(args[0], args[1], args[2])
        return area

    @staticmethod
    # @Loger
    def _triangle_area(a: float, b: float, c: float) -> float:
        p = (a + b + c) / 2
        return sqrt(p * (p - a) * (p - b) * (p - c))

    @staticmethod
    # @Loger
    def _equilateral_triangle_area(a: float) -> float:
        return sqrt(3) * a * a / 4

    @staticmethod
    # @Loger
    def _right_triangle_area(a: float, b: float) -> float:
        return b * a / 2

    @staticmethod
    # @Loger
    def _circle_area(r: float) -> float:
        return pi * r * r
