# coding:utf-8
# @author: ssh
from setuptools import setup, find_packages

NAME = 'cr_assis'

VERSION = '0.1.0'

REQUIRES = [
    'pymongo~=3.13',
    'PyGithub>=1.55',
    'ccxt>=1.91.22',
    'openpyxl>=3.0.9',
    'redis==4.3.4',
    'bokeh==2.4.3',
    'influxdb==5.3.1',
    'pymongo==3.13.0',
    'ccxt==3.0.68',
    'pandas>=2.0.0',
    'numpy>=1.24.2',
    'matplotlib>=3.7.1',
    'binance-futures-connector>=3.3.1',
    'jinja2>=3.0.0'
]

setup(
    name=NAME,  # 包名字
    version=VERSION,  # 包版本
    description='This is a `cr_assis` of the setup',  # 简单描述
    author='ssh',  # 作者
    author_email='ssh21927@gmail.com',  # 作者邮箱
    url='https://github.com/SSH-C12138/cr_assis.git',  # 包的主页
    install_requires=REQUIRES,
    packages=find_packages(),
    package_data={'': ['*.json']},
)
