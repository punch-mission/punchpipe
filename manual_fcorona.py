from datetime import datetime
from glob import glob
from punchbowl.level3.f_corona_model import construct_polarized_f_corona_model
from punchbowl.data import write_ndcube_to_fits

filenames = glob("/home/jmbhughes/data/simpunch/2/PTM/**/*.fits", recursive=True)

model = construct_polarized_f_corona_model(filenames, reference_time=datetime(2020, 1, 15).isoformat())
write_ndcube_to_fits(model[0], "fcorona2.fits")
