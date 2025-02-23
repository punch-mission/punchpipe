from punchbowl.data import load_ndcube_from_fits
import matplotlib.pyplot as plt

# l3_ptm_path = "/home/jmbhughes/data/simpunch/3/PTM/2020/01/01/PUNCH_L3_PTM_20200101002000_v1.fits"
# l3_pim_path = l3_ptm_path.replace("PTM", "PIM")
# l2_ptm_path = l3_pim_path.replace("PIM", "PTM").replace("L3", "L2").replace("/3/", '/2/')
#
# l3_ptm = load_ndcube_from_fits(l3_ptm_path)
# l3_pim = load_ndcube_from_fits(l3_pim_path)
# l2_ptm = load_ndcube_from_fits(l2_ptm_path)
#
# fig, axs = plt.subplots(ncols=3, sharex=True, sharey=True)
# axs[0].imshow(l2_ptm.data[0], vmin=1E-13, vmax=1E-12)
# axs[1].imshow(l3_pim.data[0], vmin=1E-13, vmax=1E-12)
# axs[2].imshow(l3_ptm.data[0], vmin=1E-13, vmax=1E-12)
# plt.show()

starfield = load_ndcube_from_fits("starfield.fits")

fig, ax = plt.subplots()
ax.imshow(starfield.data[0]*1000, vmin=1E-13, vmax=1E-12)
plt.show()
