"""Run with the fresh environment's Python, outside the source checkout."""

from importlib.metadata import version

from pypiron_ci_example import greeting

assert version("pypiron-ci-example") == "0.1.0"
assert version("six") == "1.17.0"
assert greeting("Ada") == "Hello, Ada! This message came from an installed wheel."
print("PASS: installed wheel, packaged resource, and public dependency")
