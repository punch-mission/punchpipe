from punchpipe.level0.tasks import multiply


def test_multiply():
    for i in range(10):
        for j in range(10):
            assert multiply(i, j) == i*j
