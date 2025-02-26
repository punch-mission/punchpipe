import glob
from datetime import datetime

from punchbowl.data import write_ndcube_to_fits
from punchbowl.level3.stellar import generate_starfield_background

pim_paths = glob.glob("/d0/punchsoc/gamera_data/3/PIM/**/*.fits", recursive=True)
starfield = generate_starfield_background(pim_paths, target_mem_usage=100,
                                          n_procs=100,
                                          reference_time=datetime(2020, 2, 1))[0]
write_ndcube_to_fits(starfield, "starfield.fits")
