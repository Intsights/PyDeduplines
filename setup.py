import setuptools
import os
import glob


class GetPybind11Include:
    def __init__(
        self,
        user,
    ):
        self.user = user

    def __str__(
        self,
    ):
        import pybind11

        return pybind11.get_include(
            user=self.user,
        )


setuptools.setup(
    name='PyDeduplines',
    version='0.1.2',
    author='Gal Ben David',
    author_email='gal@intsights.com',
    url='https://github.com/intsights/PyDeduplines',
    project_urls={
        'Source': 'https://github.com/intsights/PyDeduplines',
    },
    license='MIT',
    description='Python library for a duplicate lines removal written in C++',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    keywords='duplicates lines mimalloc hashmap hashset sort uniq unique c++',
    python_requires='>=3.6',
    zip_safe=False,
    install_requires=[
        'pybind11',
    ],
    setup_requires=[
        'pybind11',
    ],
    package_data={},
    include_package_data=True,
    ext_modules=[
        setuptools.Extension(
            name='pydeduplines',
            sources=glob.glob(
                pathname=os.path.join(
                    'src',
                    'pydeduplines.cpp',
                ),
            ),
            language='c++',
            extra_compile_args=[
                '-Ofast',
                '-std=c++17',
                '-Wno-unknown-pragmas',
                '-Wno-class-memaccess',
            ],
            extra_link_args=[],
            include_dirs=[
                'src',
                'src/mimalloc/include',
                GetPybind11Include(False),
                GetPybind11Include(True),
            ]
        ),
    ],
)
