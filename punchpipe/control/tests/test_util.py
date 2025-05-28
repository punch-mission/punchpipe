from punchpipe.control.util import load_quicklook_scaling


def test_load_quicklook_scaling():
    vmin, vmax = load_quicklook_scaling(level="0", product="CR", obscode="2", path="punchpipe/control/tests/punchpipe_config.yaml")

    assert vmin == 100
    assert vmax == 800


def test_load_quicklook_scaling_no_input():
    vmin, vmax = load_quicklook_scaling(path="punchpipe/control/tests/punchpipe_config.yaml")

    assert vmin == 5e-13
    assert vmax == 5e-11


def test_load_quicklook_scaling_no_product():
    vmin, vmax = load_quicklook_scaling(level="0", path="punchpipe/control/tests/punchpipe_config.yaml")

    assert vmin == 100
    assert vmax == 800
