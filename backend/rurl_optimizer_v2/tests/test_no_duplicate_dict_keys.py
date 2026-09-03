"""Geen dubbele string-keys in een dict-literal.

Python accepteert `{'a': 1, 'a': 2}` zonder te klagen en houdt stil de LAATSTE
over. In de cross-maincat-fallback-return stond `redirect_url` twee keer: eerst
de ruwe bestemming, daarna de geprunde. De laatste won, dus het werkte — maar wie
die regels ooit herordent of de tweede weghaalt, verliest de V61-pruning zonder
foutmelding. De klasse is goedkoper te bewaken dan het geval.
"""
import ast
import collections
import glob
import io
import os

SRC = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _duplicates(path):
    tree = ast.parse(io.open(path, encoding='utf-8').read())
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        keys = [k.value for k in node.keys
                if isinstance(k, ast.Constant) and isinstance(k.value, str)]
        for key, count in collections.Counter(keys).items():
            if count > 1:
                yield node.lineno, key


def test_no_duplicate_dict_keys():
    hits = []
    for path in glob.glob(os.path.join(SRC, '**', '*.py'), recursive=True):
        for lineno, key in _duplicates(path):
            hits.append(f"{os.path.relpath(path, SRC)}:{lineno} — '{key}'")
    assert not hits, "dubbele dict-keys:\n" + "\n".join(hits)
