"""Setup configuration for swing package."""

from setuptools import find_packages, setup

# Setup configuration for editable installs and proper package discovery
setup(
    name="swing",
    packages=find_packages(
        where=".",
        include=["src*", "backtest*", "db*", "fetch*", "screening*", "scripts*"],
    ),
    package_dir={"": "."},
    py_modules=[],
    python_requires=">=3.12",
)
