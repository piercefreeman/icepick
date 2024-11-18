from iceaxe.__tests__.helpers import pyright_raises


def test_basic_type_error():
    def type_error_func(x: int) -> int:
        return 10

    with pyright_raises("reportArgumentType"):
        type_error_func("20")  # type: ignore
