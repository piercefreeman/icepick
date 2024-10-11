from setuptools import setup, Extension
from Cython.Build import cythonize

extensions = [
    Extension("iceaxe.session_optimized", ["iceaxe/session_optimized.pyx"]),
]

setup(
    name="iceaxe",
    ext_modules=cythonize(extensions, language_level="3"),
    zip_safe=False,
)
