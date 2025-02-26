from datetime import datetime
from glob import glob
import matplotlib as mpl
mpl.use('Qt5Agg')  # o
import os

from matplotlib import pyplot as plt
from punchbowl.level3.f_corona_model import construct_polarized_f_corona_model
from punchbowl.data import write_ndcube_to_fits, load_ndcube_from_fits

filenames = glob("/d0/punchsoc/gamera_data/2/PTM/**/*.fits", recursive=True)

for filename in filenames:
    fcorona_model = load_ndcube_from_fits("fcorona.fits")

    # fig, ax = plt.subplots()
    # ax.imshow(fcorona_model.data[0], vmin=1E-13, vmax=1E-12,  interpolation='None')
    # plt.show()

    image = load_ndcube_from_fits(filename)

    # fig, ax = plt.subplots()
    # ax.imshow(image.data[0], vmin=1E-13, vmax=1E-12,  interpolation='None')
    # plt.show()

    result = image.data - fcorona_model.data

    # fig, ax = plt.subplots()
    # ax.imshow(result[0], vmin=1E-14, vmax=1E-13,  interpolation='None')
    # plt.show()

    image.data[:] = result.data[:]
    new_filename = filename.replace("/2/", "/3/").replace("_L2_", "_L3_").replace("PTM", "PIM")
    os.makedirs(os.path.dirname(new_filename), exist_ok=True)
    write_ndcube_to_fits(image, new_filename)
