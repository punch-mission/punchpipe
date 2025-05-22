from punchpipe.control.util import load_quicklook_scaling


def test_load_quicklook_scaling():
    vmin, vmax = load_quicklook_scaling(level="0", product="CR", path="punchpipe/control/tests/punchpipe_config.yaml")

    assert vmin, vmax == (400, 10000)


def test_load_quicklook_scaling_no_input():
    vmin, vmax = load_quicklook_scaling(path="punchpipe/control/tests/punchpipe_config.yaml")

    assert vmin, vmax == (1e-15, 8e-13)


def test_load_quicklook_scaling_no_product():
    vmin, vmax = load_quicklook_scaling(level="0", path="punchpipe/control/tests/punchpipe_config.yaml")

    assert vmin, vmax == (400, 10000)
