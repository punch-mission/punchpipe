# -*- coding: utf-8 -*-
"""
This module provides the core tasks for a controlsegment pipeline.
"""
from prefect import Task
from abc import abstractmethod, ABCMeta
from typing import Type, Dict, Optional, Union
from punchpipe.infrastructure.data import PUNCHData, PUNCHCalibration

CalibrationConfiguration = Dict[str, Union[PUNCHCalibration, str, int, float]]


class PipelineTask(Task):
    """
    Core task that all other pipeline tasks get inherited from. It allows us to have a modification of the
    prefect Task class for custom functionality easily.

    Attributes
    -----------
    name : str
        Name of the task for future reference.
    **kwargs : dict
        Prefect defines many things on a Task. See their documentation for a full listing.

    Methods
    -------
    run(**kwargs)
        Run the task

    """
    def __init__(self, name, **kwargs):
        """Creates the Task object.

        Parameters
        ----------
        name : str
            Name of the Task.
        **kwargs : dict
            Optional parameters provided by Prefect's Task class.
        """
        super().__init__(checkpoint=False, **kwargs)
        self.name = name

    @abstractmethod
    def run(self, **kwargs):
        """Runs the Task.

        Parameters
        ----------
        **kwargs : dict
            Run specific keywords provided by Prefect.

        Returns
        -------
        None
        """
        pass


class ScienceFunction(metaclass=ABCMeta):
    """Class science code writers inherit from to create a new pipeline item
    """
    def __init__(self):
        pass

    def process(self, data_object: PUNCHData,
                parameters: Optional[CalibrationConfiguration] = None) -> PUNCHData:
        """Main method to run a science function.

        Parameters
        ----------
        data_object : DataObject
            Input data for the operation.
        parameters : CalibrationConfiguration, Optional
            Parameters used in the ScienceFunction's execution, e.g. some look-up table for calibration.

        Returns
        -------
        DataObject
            Modified data object.
        """
        pass


class ScienceTask(PipelineTask):
    """Task for completing science functions
    """
    def __init__(self, name: str, science_function: Type[ScienceFunction],
                 parameters: Optional[CalibrationConfiguration] = None, **kwargs):
        """Create a ScienceTask.

        Parameters
        ----------
        name : str
            Name of the task.
        science_function : ScienceFunction
            Function that should be executed by the task.
        parameters : CalibrationConfiguration, Optional
            Parameters used in the execution of the science_function.
        **kwargs : dict
            Other Prefect keywords.
        """
        super().__init__(name, **kwargs)
        self.science_function = science_function
        self.parameters = parameters

    def run(self, data_object: PUNCHData):  # parameters: Optional[CalibrationConfiguration]):
        """ Run the task.
        Parameters
        ----------
        data_object : DataObject
            Data to run the task on.

        Returns
        -------
        DataObject
            Data modified by the task.

        """
        return self.science_function().process(data_object)


class IngestTask(PipelineTask):
    """A special kind of task for loading data.
    """
    def __init__(self, name, **kwargs):
        """Create a task to load data.

        Parameters
        ----------
        name : str
            Name of the task.
        **kwargs : dict
            Other Prefect keywords.
        """
        super().__init__(name, **kwargs)

    @abstractmethod
    def open(self, path: str) -> PUNCHData:
        """Method to open the data and return it as a data object.
        Parameters
        ----------
        path : str
            Path to data on the drive.

        Returns
        -------
        DataObject
            Specific kind of data opened as a data object.
        """
        pass

    def run(self, path: str):
        """Executes the open operation and returns the data.
        Parameters
        ----------
        path : str
            Path to the data to open.

        Returns
        -------
        DataObject
            Opened data file.
        """
        return self.open(path)


class OutputTask(PipelineTask):
    """ Writes the results to a file.
    """
    def __init__(self, name, **kwargs):
        """Creates an OutputTask.
        Parameters
        ----------
        name : str
            Name of the task.
        **kwargs : dict
            Other prefect keywords.
        """
        super().__init__(name, **kwargs)

    @abstractmethod
    def write(self, data: PUNCHData, path: str) -> None:
        """Instructions on how to write the data to the specific file path
        Parameters
        ----------
        data: DataObject
            Data which should be written to file.
        path : str
            Where to write the data on disk.

        Returns
        -------
        None

        """
        pass

    def run(self, data: PUNCHData, path: str):
        """Executes the writing method.
        Parameters
        ----------
        data : DataObject
            The specific data that should be written to file.
        path : str
            Where to write the file.
        """
        return self.write(data, path)
