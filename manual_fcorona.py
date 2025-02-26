from datetime import datetime
from glob import glob
from punchbowl.level3.f_corona_model import construct_polarized_f_corona_model
from punchbowl.data import write_ndcube_to_fits

filenames = glob("/d0/punchsoc/gamera_data/2/PTM/**/*.fits", recursive=True)
print(len(filenames))
model = construct_polarized_f_corona_model(filenames, smooth_level=3.0, reference_time=datetime(2020, 2, 1).isoformat())
write_ndcube_to_fits(model[0], "fcorona_3.fits")
