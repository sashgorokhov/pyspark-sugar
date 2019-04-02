from setuptools import setup


with open('README.md') as readme:
    long_description = readme.read()


setup(
    install_requires=['pyspark'],
    keywords=['pyspark'],
    name='pyspark_sugar',
    version='0.1',
    py_modules=['pyspark_sugar'],
    url='',
    license='',
    author='sashgorokhov',
    author_email='sashgorokhov@gmail.com',
    description='SparkUI enchancements from pyspark side',
    long_description=long_description,
)
