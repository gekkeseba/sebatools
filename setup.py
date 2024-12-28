from setuptools import setup, find_packages

setup(
    name='sebatools',     # Package name
    version='0.3.0',                  # Initial version
    description='A package to remove emojis from text and more...',
    author='Sebastiaan Hendriks',               # Your name
    author_email='sgtahendriks@gmail.com',
    packages=find_packages(),         # Automatically find packages
    python_requires='>=3.9',          # Python version compatibility
)