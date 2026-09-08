from importlib.resources import files

import six


def greeting(name: str) -> str:
    template = six.ensure_str(files(__package__).joinpath("greeting.txt").read_bytes())
    return template.strip().format(name=name)
