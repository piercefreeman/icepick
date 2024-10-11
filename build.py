from setuptools import setup, Extension
from Cython.Build import cythonize

def build(setup_kwargs):
    extensions = [
        Extension("iceaxe.session_optimized", ["iceaxe/session_optimized.pyx"]),
    ]

    setup_kwargs.update({
        "ext_modules": cythonize(
            extensions,
            compiler_directives={'language_level': "3"}
        ),
    })
