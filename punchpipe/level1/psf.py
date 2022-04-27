from typing import Optional
from datetime import datetime
from punchpipe.infrastructure.tasks.core import ScienceFunction
from punchpipe.infrastructure.data import PUNCHData
from punchpipe.infrastructure.tasks.core import CalibrationConfiguration


class PSFCorrectionFunction(ScienceFunction):

    def process(self, data_object: PUNCHData, configuration: Optional[CalibrationConfiguration] = None) -> PUNCHData:
        data_object.add_history(datetime.now(), "LEVEL1-PSF", "psf corrected")
        return data_object  # TODO : actually do psf correction!
