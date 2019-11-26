# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import re

from setuptools import setup, find_packages

v = open(os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])), 'sqlalchemy_solr', '__init__.py'))
VERSION = re.compile(r".*__version__ = '(.*?)'", re.S).match(v.read()).group(1)
v.close()

readme = os.path.join(os.path.dirname(__file__), 'README.md')

setup(name='sqlalchemy_solr',
      version=VERSION,
      description="Apache Solr for SQLAlchemy",
      long_description=open(readme).read(),
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Environment :: Console',
          'Intended Audience :: Developers',
          'Programming Language :: Python',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: Implementation :: CPython',
          'Topic :: Database :: Front-Ends',
      ],
      install_requires=[
          "requests",
          "numpy",
          "pandas",
          "sqlalchemy"
      ],
      extras_require={
          "jdbc": ["JPype1==0.6.3", "JayDeBeApi"]
      },
      keywords='SQLAlchemy Apache Solr',
      author='Ahmed Adel',
      author_email='hello@aadel.io',
      license='Apache',
      packages=find_packages(),
      include_package_data=True,
      tests_require=['nose >= 0.11'],
      test_suite="nose.collector",
      zip_safe=False,
      entry_points={
          'sqlalchemy.dialects': [
              'solr = sqlalchemy_solr.http:SolrDialect_http',
              'solr.http = sqlalchemy_solr.http:SolrDialect_http',
          ]
      }
    )
