from setuptools import setup

setup(
    name="pynats",
    version="0.0.1",
    packages=["pynats", "pynats.protocol"],
    zip_safe=True,
    long_description="A synchronous NATS client, because not everyone's codebase is built for asyncio",
)
