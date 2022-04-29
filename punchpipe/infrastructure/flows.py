from __future__ import annotations
from abc import ABCMeta, abstractmethod
from prefect import Flow, Parameter
from prefect.tasks.mysql import MySQLFetch
from prefect.tasks.prefect.flow_run_rename import RenameFlowRun
from prefect.schedules import IntervalSchedule
from prefect.triggers import any_failed
from typing import Optional, List, Dict, Set, Tuple, NewType
import graphviz
from datetime import timedelta, datetime
from punchpipe.infrastructure.controlsegment import DatabaseCredentials, DatabaseCredentials
from punchpipe.infrastructure.tasks.core import PipelineTask, OutputTask
from punchpipe.infrastructure.tasks.processor import MarkFlowAsRunning, MarkFlowAsEnded, MarkFlowStartTime, MarkFlowEndTime, \
    CreateFileDatabaseEntry, MarkFlowAsFailed
from punchpipe.infrastructure.tasks.launcher import GatherQueuedFlows, CountRunningFlows, \
    EscalateLongWaitingFlows, FilterForLaunchableFlows, LaunchFlow
from punchpipe.infrastructure.tasks.scheduler import CheckForInputs, ScheduleFlow, ScheduleFile


__all__ = ['FlowGraph',
           'FlowBuilder',
           'CoreFlowBuilder',
           'LauncherFlowBuilder',
           'SchedulerFlowBuilder',
           'ProcessFlowBuilder',
           'KeywordDict']

KeywordDict = NewType("KeywordDict", Dict[Tuple[PipelineTask, PipelineTask], Optional[str]])


class FlowGraph:
    """Representation of a directed acyclic graph (DAG) that can be read by `FlowBuilder` classes to create
    various `prefect Flow` objects.

    Attributes
    ----------
    segment_id : int
        The level associated with this segment
    segment_description : str
        A description of what this flow does
    """
    def __init__(self, segment_id: int, segment_description: str,
                 adj_list: Optional[Dict[PipelineTask | None, List[PipelineTask]]] = None,
                 keywords: Optional[KeywordDict] = None):
        """Initialize a flow graph

        Parameters
        ----------
        segment_id : int
            The level associated with this segment
        segment_description : str
            A description of what this flow does
        adj_list: Optional[Dict[Task | None, List[Task]]]
            Adjacency list representation of the graph
        keywords: Optional[KeywordDict]
            Mapping that indicates which tasks are used as keyword parameters for other tasks in the flow
        """
        self.segment_id: int = segment_id
        self.segment_description: str = segment_description

        # save the adjacency list representation
        if adj_list:
            self._adj_list: Dict[PipelineTask | None, List[PipelineTask]] = adj_list
        else:
            self._adj_list: Dict[PipelineTask | None, List[PipelineTask]] = dict()

        # accumulate a set of all tasks
        self._tasks: Set[PipelineTask] = set()
        for start_task, end_task_list in self._adj_list.items():
            if start_task is not None:
                self._tasks.add(start_task)
            for end_task in end_task_list:
                self._tasks.add(end_task)

        # set up the keyword dict
        if keywords:
            self._keywords: KeywordDict = keywords
        else:
            self._keywords: KeywordDict = KeywordDict(dict())

    def add_task(self, current_task: PipelineTask,
                 prior_tasks: Optional[List[PipelineTask]] = None,
                 keywords: Optional[KeywordDict] = None) -> None:
        """Adds a task to the flow graph.

        Parameters
        ----------
        current_task : PipelineTask
            The task that is being added.
        prior_tasks : Optional[List[PipelineTask]]
            Tasks that directly preceed the `current_task` in execution.
        keywords : Optional[KeywordDict]
            Mapping that dictates which of the `prior_tasks` are used as keywords input to the `current_task`.

        Returns
        -------
        None
        """

        # If they're none just fill them in with defaults
        if prior_tasks is None:
            prior_tasks = []

        if keywords is None:
            keywords = dict()

        if prior_tasks:
            assert all((task in self._adj_list for task in prior_tasks)), \
                "One of the prior tasks does not exist in the graph."
            for prior_task in prior_tasks:
                self._adj_list[prior_task].append(current_task)
        else:  # task has no priors
            if None in self._adj_list:  # A task with no priors already exists
                self._adj_list[None].append(current_task)
            else:  # no tasks with no priors exists
                self._adj_list[None] = [current_task]
        self._adj_list[current_task] = []

        for start_task, keyword in keywords.items():
            self._keywords[(start_task, current_task)] = keyword

        self._tasks.add(current_task)

    @staticmethod
    def _expand_keywords(prior_tasks: List[PipelineTask],
                         keywords: KeywordDict) -> None:
        for prior_task in prior_tasks:
            if prior_task not in keywords:
                keywords[prior_task] = None

    @staticmethod
    def _validate_keyword_prior_task_matches(prior_tasks: List[PipelineTask],
                                             keywords: KeywordDict) -> bool:
        pass

    def render(self, path: str) -> None:
        """Makes a graphical representation of the FlowGraph.

        Parameters
        ----------
        path : str
            where to write out the graphical representation.

        Returns
        -------
        None
        """
        g = graphviz.Digraph(self.segment_description)
        for task in self._tasks:
            g.node(task.name)
        for start_node, end_node in self.iterate_edges():
            if start_node is not None:
                if (start_node, end_node) in self._keywords:
                    keyword = self._keywords[(start_node, end_node)]
                else:
                    keyword = None
                keyword = "" if keyword is None else keyword
                g.edge(start_node.name, end_node.name, label=keyword)
        g.render(filename=path)

    def __len__(self) -> int:
        """Number of tasks in the flow graph.
        Returns
        -------
        int
            Number of tasks in the flow graph.
        """
        return len(self._tasks)

    def iterate_tasks(self) -> PipelineTask:
        """Iterates over tasks in the FlowGraph.

        Yields
        -------
        PipelineTask
            the next pipeline task in the graph.
        """
        for task in self._tasks:
            yield task

    def iterate_edges(self) -> Tuple[PipelineTask, PipelineTask]:
        """Iterates over the edges in the graph.

        Yields
        -------
        Tuple[PipelineTask, PipelineTask]
            an edge in the graph where the first entry in the tuple is the start node and the last is the end node.
        """
        for start_task in self._adj_list:
            for end_task in self._adj_list[start_task]:
                yield start_task, end_task

    def get_downstream_tasks(self, task: PipelineTask) -> List[PipelineTask]:
        """Determines downstream tasks for a given task. Downstream means a task that should be executed after.

        Parameters
        ----------
        task : PipelineTask
            Task for which to get the downstream tasks for.

        Returns
        -------
        List[PipelineTask]
            All downstream tasks. Will be empty if there are none.
        """
        return self._adj_list[task] if task in self._adj_list else []

    def get_upstream_tasks(self, task: PipelineTask) -> List[PipelineTask]:
        """Determines upstream task for a given task. Upstream means a task that should execute before the given task.

        Parameters
        ----------
        task : PipelineTask
            Query task that is used as a reference for finding those upstream

        Returns
        -------
        List[PipelineTask]
            All upstream tasks
        """
        pass

    def gather_keywords_into(self, task: PipelineTask) -> Dict[str, PipelineTask]:
        """Collects the keyword tasks that go into a given task as a dictionary. See the module description for
        what a keyword task is.

        Parameters
        ----------
        task : PipelineTask
            Query task that is the end node for all the incoming tasks.

        Returns
        -------
        Dict[str, PipelineTask]
            Keys are the keywords and the value is the task that is upstream of the queried task.
        """
        output: Dict[str, PipelineTask] = dict()
        for (start_task, end_task), keyword in self._keywords.items():
            if task == end_task:
                if keyword:
                    output[keyword] = start_task
        return output


class FlowBuilder(metaclass=ABCMeta):
    """A generic builder class that the specific types of flows get constructed by.

    Attributes
    -----------
    database_credentials : DatabaseCredentials
        Database name, user, and password for the ControlSegment's database
    flow_name : str
        Name of the flow that will be built
    frequency_in_minutes : Optional[int]
        How often the given flow should run. If None, it does not run on a schedule but only when called.

    See Also
    ---------
    CoreFlowBuilder, LauncherFlowBuilder, SchedulerFlowBuilder, ProcessFlowBuilder : children that implement this

    """
    def __init__(self, database_credentials: DatabaseCredentials,
                 flow_name: str,
                 frequency_in_minutes: Optional[int] = None):
        self.database_credentials = database_credentials
        self.flow_name = flow_name
        self.frequency_in_minutes = frequency_in_minutes

    @abstractmethod
    def build(self) -> Flow:
        pass


class CoreFlowBuilder(FlowBuilder):
    """A flow builder that takes a flow graph to create core flows. These core flows, by definition, should not touch
    the databases.
    """
    def __init__(self, level: int, flow_graph: FlowGraph):
        super().__init__(None, f"core level {level}")
        self.flow_graph = flow_graph
        self.level = level

    def build(self) -> Flow:
        flow = Flow(self.flow_name)

        # Add all tasks to the flow
        for task in self.flow_graph.iterate_tasks():
            flow.add_task(task)

        # Create the dependencies between tasks
        for start_task in self.flow_graph.iterate_tasks():
            flow.set_dependencies(start_task,
                                  keyword_tasks=self.flow_graph.gather_keywords_into(start_task),
                                  downstream_tasks=self.flow_graph.get_downstream_tasks(start_task))

        return flow


class LauncherFlowBuilder(FlowBuilder):
    """A flow builder that makes launcher flows, i.e. the flows that start process flows running.
    """
    def __init__(self, database_credentials: DatabaseCredentials, frequency_in_minutes: int):
        super().__init__(database_credentials, "launcher")
        self.frequency_in_minutes = frequency_in_minutes

    def build(self) -> Flow:
        # Prepare the schedule
        schedule = IntervalSchedule(interval=timedelta(minutes=self.frequency_in_minutes))

        flow = Flow(self.flow_name, schedule=schedule)

        # Create all necessary tasks and add them to the flow
        gather_queued_flows = GatherQueuedFlows(user=self.database_credentials.user,
                                                password=self.database_credentials.password,
                                                db_name=self.database_credentials.project_name,
                                                fetch='all')
        count_running_flows = CountRunningFlows(user=self.database_credentials.user,
                                                password=self.database_credentials.password,
                                                db_name=self.database_credentials.project_name)
        escalate_long_waiting_flows = EscalateLongWaitingFlows(user=self.database_credentials.user,
                                                               password=self.database_credentials.password,
                                                               db_name=self.database_credentials.project_name)
        filter_for_launchable_flows = FilterForLaunchableFlows()
        launch_flow = LaunchFlow()

        flow.add_task(gather_queued_flows)
        flow.add_task(count_running_flows)
        flow.add_task(escalate_long_waiting_flows)
        flow.add_task(filter_for_launchable_flows)
        flow.add_task(launch_flow)

        # Build the dependencies between tasks
        flow.set_dependencies(count_running_flows,
                              downstream_tasks=[filter_for_launchable_flows])
        flow.set_dependencies(escalate_long_waiting_flows,
                              downstream_tasks=[gather_queued_flows])
        flow.set_dependencies(gather_queued_flows,
                              downstream_tasks=[filter_for_launchable_flows])
        flow.set_dependencies(filter_for_launchable_flows,
                              downstream_tasks=[launch_flow],
                              keyword_tasks=dict(running_flow_count=count_running_flows,
                                                 priority_sorted_queued_flows=gather_queued_flows))
        flow.set_dependencies(launch_flow,
                              downstream_tasks=[],
                              keyword_tasks=dict(flow_entry=filter_for_launchable_flows),
                              mapped=True)
        return flow


class SchedulerFlowBuilder(FlowBuilder):
    """A flow builder that creates scheduler flows, i.e. the kinds of flows that look for ready files and schedule
    process flows to process them.
    """
    def __init__(self, database_credentials: DatabaseCredentials,
                 level: int, frequency_in_minutes: int, inputs_check_task: CheckForInputs, query_task: MySQLFetch):
        super().__init__(database_credentials, f"scheduler level {level}")
        self.level = level
        self.frequency_in_minutes = frequency_in_minutes
        self.query_task = query_task(user=self.database_credentials.user,
                                     password=self.database_credentials.password,
                                     db_name=self.database_credentials.project_name,
                                     fetch='all')
        self.inputs_check_task = inputs_check_task()

    def build(self) -> Flow:
        schedule_flow = ScheduleFlow(user=self.database_credentials.user,
                                     password=self.database_credentials.password,
                                     db_name=self.database_credentials.project_name,
                                     commit=True)
        schedule_file = ScheduleFile(user=self.database_credentials.user,
                                     password=self.database_credentials.password,
                                     db_name=self.database_credentials.project_name,
                                     commit=True)

        schedule = IntervalSchedule(interval=timedelta(minutes=self.frequency_in_minutes))
        flow = Flow(self.flow_name, schedule=schedule)
        flow.add_task(self.query_task)
        flow.add_task(self.inputs_check_task)
        flow.add_task(schedule_flow)
        # flow.add_task(schedule_file)

        flow.set_dependencies(self.query_task,
                              downstream_tasks=[self.inputs_check_task])
        flow.set_dependencies(self.inputs_check_task,
                              downstream_tasks=[schedule_flow],
                              keyword_tasks=dict(query_result=self.query_task))
        flow.set_dependencies(schedule_flow,
                              keyword_tasks=dict(pair=self.inputs_check_task),
                              mapped=True)

        # TODO: re-enable scheduling file writing
        # flow.set_dependencies(schedule_file,
        #                       keyword_tasks=dict(pair=self.inputs_check_task),
        #                       mapped=True)
        return flow


class ProcessFlowBuilder(FlowBuilder):
    """A flow builder that converts core flows into process flows.
    """
    def __init__(self, database_credentials: DatabaseCredentials, level: int, core_flow: Flow) -> None:
        super().__init__(database_credentials, f"processor level {level}")
        self.core_flow = core_flow
        self.level = level

        self.core_flow_parameters = self.core_flow.parameters()

        # Construct the entry tasks.
        # A task is an entry task if it is a non-Parameter root task, i.e. a task with no upstream tasks that is also
        # not a parameter, or if it is a Parameter's direct downstream task.
        self.entry_tasks = set()
        for task in self.core_flow.root_tasks():
            if isinstance(task, Parameter):
                self.entry_tasks = self.entry_tasks.union(self.core_flow.downstream_tasks(task))
            else:
                self.entry_tasks.add(task)

        # Construct ending tasks. These are simply the terminal tasks of the core flow.
        self.ending_tasks = self.core_flow.terminal_tasks()

        # Tasks that are output tasks
        self.output_tasks = self.core_flow.get_tasks(task_type=OutputTask)

    def build(self) -> Flow:
        process_flow = self.core_flow.copy()
        process_flow.name = process_flow.name.replace("core", "process")
        flow_id = Parameter("flow_id")

        # set up the pre-flow tasks
        rename_flow = RenameFlowRun()
        mark_as_running = MarkFlowAsRunning(user=self.database_credentials.user,
                                     password=self.database_credentials.password,
                                     db_name=self.database_credentials.project_name,
                                     commit=True)
        mark_flow_start_time = MarkFlowStartTime(user=self.database_credentials.user,
                                     password=self.database_credentials.password,
                                     db_name=self.database_credentials.project_name,
                                     commit=True)

        process_flow.set_dependencies(
            rename_flow,
            downstream_tasks=[mark_as_running],
            keyword_tasks=dict(flow_run_name=flow_id)
        )
        process_flow.set_dependencies(
            mark_as_running,
            downstream_tasks=[mark_flow_start_time],
            keyword_tasks=dict(flow_id=flow_id)
        )
        process_flow.set_dependencies(
            mark_flow_start_time,
            downstream_tasks=self.entry_tasks,
            keyword_tasks=dict(flow_id=flow_id)
        )

        # set up the post-flow tasks
        mark_as_ended = MarkFlowAsEnded(user=self.database_credentials.user,
                                     password=self.database_credentials.password,
                                     db_name=self.database_credentials.project_name,
                                     commit=True)
        mark_flow_end_time = MarkFlowEndTime(user=self.database_credentials.user,
                                     password=self.database_credentials.password,
                                     db_name=self.database_credentials.project_name,
                                     commit=True)
        # TODO: re-enable writing to database for completed tasks
        # create_database_entry_tasks = [CreateFileDatabaseEntry() for _ in self.output_tasks]

        process_flow.set_dependencies(
            mark_flow_end_time,
            upstream_tasks=self.ending_tasks,
            keyword_tasks=dict(flow_id=flow_id)
        )

        process_flow.set_dependencies(
            mark_as_ended,
            upstream_tasks=[mark_flow_end_time],
            keyword_tasks=dict(flow_id=flow_id)
        )

        # TODO: re-enable writing to database for completed files
        # for database_task, output_task in zip(create_database_entry_tasks, self.output_tasks):
        #     print(database_task, output_task)
        #     process_flow.set_dependencies(
        #         database_task,
        #         upstream_tasks=[output_task],
        #         keyword_tasks=dict(flow_id=flow_id, meta_data=output_task)
        #     )

        # Set up the graceful exit if something went wrong in executing the flow
        # We add a task that follows all core flow and database tasks
        # and marks the flow as failed if any of the prior tasks fail
        # It does require the flow_id to know which flow failed
        existing_tasks = process_flow.get_tasks()
        core_failure_task = MarkFlowAsFailed(trigger=any_failed, user=self.database_credentials.user,
                                     password=self.database_credentials.password,
                                     db_name=self.database_credentials.project_name,
                                     commit=True)
        process_flow.add_task(core_failure_task)
        process_flow.set_dependencies(core_failure_task, upstream_tasks=existing_tasks,
                                      keyword_tasks=dict(flow_id=flow_id))

        # The process flow is successful or not depending on the status of the core tasks
        process_flow.set_reference_tasks(self.core_flow.get_tasks())

        return process_flow

