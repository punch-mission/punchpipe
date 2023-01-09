# version stuff copied from https://stackoverflow.com/a/17638236
from pkg_resources import get_distribution, DistributionNotFound
import os.path

try:
    _dist = get_distribution('punchpipe')
    # Normalize case for Windows systems
    dist_loc = os.path.normcase(_dist.location)
    here = os.path.normcase(__file__)
    if not here.startswith(os.path.join(dist_loc, 'punchpipe')):
        # not installed, but there is another version that *is*
        raise DistributionNotFound
except DistributionNotFound:
    __version__ = 'Please install punchpipe project with setup.py'
else:
    __version__ = _dist.version