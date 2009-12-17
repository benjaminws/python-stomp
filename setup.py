from setuptools import setup, find_packages
from stompy import distmeta

setup(name='stompy',
      version=distmeta.__version__,
      description=distmeta.__doc__,
      long_description=distmeta.__long_description__,
      author=distmeta.__author__,
      author_email=distmeta.__contact__,
      packages = ['stompy'],
      license='BSD',
      url=distmeta.__homepage__,
      keywords='stomp activemq jms messaging',
      test_suite="nose.collector",
      setup_requires=['nose>=0.11', 'dingus'],
      classifiers=["Development Status :: 5 - Production/Stable",
                   "Intended Audience :: Developers",
                   "License :: OSI Approved :: BSD License",
                   "Operating System :: OS Independent",
                   "Programming Language :: Python",
                   "Topic :: Software Development :: Libraries",
                   ],
     )
