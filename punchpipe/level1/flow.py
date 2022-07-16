from punchpipe.level1.alignment import align
from punchpipe import flow, get_run_logger, task
from punchpipe.infrastructure.data import PUNCHData
from punchpipe.level1.quartic_fit import perform_quartic_fit
from punchpipe.level1.despike import despike
from punchpipe.level1.destreak import destreak
from punchpipe.level1.vignette import correct_vignetting
from punchpipe.level1.deficient_pixel import remove_deficient_pixels
from punchpipe.level1.stray_light import remove_stray_light
from punchpipe.level1.alignment import align
from punchpipe.level1.psf import correct_psf
from punchpipe.level1.flagging import flag

@task
def load_level0(input_filename):
    return PUNCHData.from_fits(input_filename)

@task
def output_level1(data, output_directory):
    kind = list(data._cubes.keys())[0]

    data[kind].meta['OBSRVTRY'] = 'Z'
    data[kind].meta['LEVEL'] = 1
    data[kind].meta['TYPECODE'] = 'ZZ'
    data[kind].meta['VERSION'] = 1
    data[kind].meta['SOFTVERS'] = 1
    data[kind].meta['DATE-OBS'] = str(datetime.now())
    data[kind].meta['DATE-AQD'] = str(datetime.now())
    data[kind].meta['DATE-END'] = str(datetime.now())
    data[kind].meta['POL'] = 'Z'
    data[kind].meta['STATE'] = 'finished'
    data[kind].meta['PROCFLOW'] = '?'

    return data.write(output_directory + data.generate_id() + ".fits")

@flow
def level1_core_flow(input_filename, output_directory):
    logger = get_run_logger()

    logger.info("beginning level 1 core flow")
    data = load_level0(input_filename)
    data = perform_quartic_fit(data)
    data = despike(data)
    data = destreak(data)
    data = correct_vignetting(data)
    data = remove_deficient_pixels(data)
    data = remove_stray_light(data)
    data = align(data)
    data = correct_psf(data)
    data = flag(data)
    logger.info("ending level 1 core flow")
    output_level1(data, output_directory)

