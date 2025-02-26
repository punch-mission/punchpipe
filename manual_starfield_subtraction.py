from glob import glob
import os

from punchbowl.data import load_ndcube_from_fits, write_ndcube_to_fits
from punchbowl.level3.stellar import subtract_starfield_background_task

def do_it(cube_path, starfield_path):
    cube = load_ndcube_from_fits(cube_path)
    subtracted = subtract_starfield_background_task(cube, starfield_path)
    return subtracted

i = 20
pim_paths = sorted(glob("/d0/punchsoc/gamera_data/3/PIM/**/*.fits", recursive=True))
subtracted = do_it(pim_paths[i], "starfield.fits")
new_path = pim_paths[i].replace("PIM", "PTM")
os.makedirs(os.path.dirname(new_path), exist_ok=True)
write_ndcube_to_fits(subtracted, new_path)
