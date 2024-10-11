# Copyright 2016 Quora, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import setup
from setuptools.extension import Extension

import glob
import os.path


CYTHON_MODULES = [
    "async_task",
    "asynq_to_async",
    "batching",
    "contexts",
    "_debug",
    "decorators",
    "futures",
    "profiler",
    "scheduler",
    "scoped_value",
    "utils",
]


DATA_FILES = (
    ["py.typed"]
    + ["%s.pxd" % module for module in CYTHON_MODULES]
    + [os.path.relpath(f, "asynq/") for f in glob.glob("asynq/*.pyi")]
)

VERSION = "1.6.0"


EXTENSIONS = [
    Extension("asynq.%s" % module, ["asynq/%s.py" % module])
    for module in CYTHON_MODULES
]


if __name__ == "__main__":
    for extension in EXTENSIONS:
        extension.cython_directives = {"language_level": "3"}

    with open("./README.rst", encoding="utf-8") as f:
        long_description = f.read()

    setup(
        name="asynq",
        version=VERSION,
        author="Quora, Inc.",
        author_email="asynq@quora.com",
        description="Quora's asynq library",
        long_description=long_description,
        long_description_content_type="text/x-rst",
        url="https://github.com/quora/asynq",
        license="Apache Software License",
        classifiers=[
            "License :: OSI Approved :: Apache Software License",
            "Programming Language :: Python",
            "Programming Language :: Python :: 3.9",
            "Programming Language :: Python :: 3.10",
            "Programming Language :: Python :: 3.11",
            "Programming Language :: Python :: 3.12",
            "Programming Language :: Python :: 3.13",
        ],
        keywords="quora asynq common utility",
        packages=["asynq", "asynq.tests"],
        package_data={"asynq": DATA_FILES},
        ext_modules=EXTENSIONS,
        setup_requires=["Cython", "qcore", "setuptools"],
        install_requires=["qcore", "pygments"],
        python_requires=">=3.9",
    )
