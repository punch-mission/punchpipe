from typing import Optional
from punchpipe.infrastructure.tasks.core import ScienceFunction
from punchpipe.infrastructure.data import PUNCHData
from punchpipe.infrastructure.tasks.core import CalibrationConfiguration


class QuarticFitFunction(ScienceFunction):

    def process(self, data_object: PUNCHData, configuration: Optional[CalibrationConfiguration] = None) -> PUNCHData:
        return data_object  # TODO : actually do quartic fit correction!
