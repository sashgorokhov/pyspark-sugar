from setuptools import setup


with open('README.md') as readme:
    long_description = readme.read()


setup(
    install_requires=['pyspark'],
    keywords=['pyspark'],
    name='pyspark_sugar',
    version='0.4.1',
    py_modules=['pyspark_sugar'],
    url='https://github.com/sashgorokhov/pyspark-sugar',
    license='MIT',
    author='sashgorokhov',
    author_email='sashgorokhov@gmail.com',
    description='SparkUI enchancements with pyspark',
    long_description=long_description,
)
