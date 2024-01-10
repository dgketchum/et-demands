from setuptools import setup, find_packages

setup(
    name='et-demands',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'pandas',
    ],
    entry_points={
        'console_scripts': [
            'et-demands-field = fieldET.obs_field_cycle:main',
        ],
    },
)

if __name__ == '__main__':
    pass
# ========================= EOF ====================================================================
