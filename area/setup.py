from setuptools import setup, find_packages

print(find_packages())

def readme():
    with open('README.md', 'r') as f:
        return f.read()

setup(
    name='areaaa',
    version='0.0.1',
    author='karleneg',
    author_email='kolya.gar.1@gmail.com',
    description='This is the simplest modul for calculate figure area.',
    long_description=readme(),
    long_description_content_type='text/markdown',
    packages=['area'],
    install_requires=['pytest==8.1.1', 'loguru==0.7.2'],
    python_requires='>=3.11'
)
