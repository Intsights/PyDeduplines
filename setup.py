import setuptools
import os
import platform
import glob


extra_compile_args = []
if platform.system() == 'Windows':
    extra_compile_args.append('/std:c++17')
elif platform.system() == 'Linux':
    extra_compile_args.append('-std=c++17')
elif platform.system() == 'Darwin':
    extra_compile_args.append('-std=c++17')
    extra_compile_args.append('-stdlib=libc++')


setuptools.setup(
    name='PyDeduplines',
    version='0.2.0',
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
    package_data={},
    include_package_data=True,
    setup_requires=[
        'pytest-runner',
    ],
    tests_require=[
        'pytest',
    ],
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
            extra_compile_args=extra_compile_args,
            extra_link_args=[],
            include_dirs=[
                'src',
            ]
        ),
    ],
)
