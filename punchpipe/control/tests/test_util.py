from punchpipe.control.util import load_quicklook_scaling


def test_load_quicklook_scaling():
    vmin, vmax = load_quicklook_scaling()

    assert vmin == 400
    assert vmax == 8000
