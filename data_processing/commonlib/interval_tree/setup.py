try:
    from setuptools import setup
    from setuptools import Extension
except ImportError:
    from distutils.core import setup
    from distutils.extension import Extension
import numpy

from Cython.Build import cythonize

#setup(
#    name = "Test app",
#    ext_modules = cythonize('convolve.pyx'),
#    include_dirs=[numpy.get_include()]
#)

extensions = [
    Extension("intersection", ["intersection.pyx"],
              extra_compile_args=["-Zi", "/Ox"], include_dirs=[numpy.get_include()])]
setup(
    name = "My interval app",
    ext_modules = cythonize(extensions),
)