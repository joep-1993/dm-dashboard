"""De facetwaarde-namen als weergavestring: geen dubbele, geen lege.

`ruimte_woonaccessoires~19960809~~t_vloerkleed~6993703` leverde
"Vloerkleden Buiten, Buiten" op in de reviewsheet: twee verschillende assen met
dezelfde waardenaam. De URL heeft beide assen nodig, de naam maar één keer.
"""
import os, sys
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)

from main_parallel_v2 import _join_value_names as join


def test_the_vloerkleden_buiten_case():
    assert join(['Buiten', 'Buiten']) == 'Buiten'


def test_keeps_order_and_the_first_spelling():
    assert join(['Buiten', 'Rond', 'buiten']) == 'Buiten, Rond'


def test_drops_empty_and_whitespace_only():
    assert join(['Buiten', '', None, '   ', 'Rond']) == 'Buiten, Rond'


def test_distinct_names_are_all_kept():
    assert join(['Zwart', 'Kunststof', 'Rond']) == 'Zwart, Kunststof, Rond'


def test_empty_input_is_an_empty_string():
    assert join([]) == ''
    assert join(None or []) == ''
