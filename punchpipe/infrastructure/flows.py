from __future__ import annotations
from abc import ABCMeta, abstractmethod
from prefect import Flow, Parameter
from prefect.tasks.prefect.flow_run_rename import RenameFlowRun
from prefect.schedules import IntervalSchedule
from typing import Optional, List, Dict, Set, Tuple, NewType
import graphviz
from datetime import timedelta, datetime
from punchpipe.infrastructure.controlsegment import DatabaseCredentials, DatabaseCredentials
from punchpipe.infrastructure.tasks.core import PipelineTask, OutputTask
from punchpipe.infrastructure.tasks.processor import MarkFlowAsRunning, MarkFlowAsEnded, MarkFlowStartTime, MarkFlowEndTime, \
    CreateFileDatabaseEntry
from punchpipe.infrastructure.tasks.launcher import GatherQueuedFlows, CountRunningFlows, \
    EscalateLongWaitingFlows, FilterForLaunchableFlows, LaunchFlow
from punchpipe.infrastructure.tasks.scheduler import CheckForInputs, ScheduleFlow


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
            # TODO: validate the keyword list
        else:
            self._keywords: KeywordDict = KeywordDict(dict())
            # for start_task, end_task_list in self._adj_list.items():
            #     for end_task in end_task_list:
            #         self._keywords[(start_task, end_task)] = None

    def add_task(self, current_task: PipelineTask,
                 prior_tasks: Optional[List[PipelineTask]] = None,
                 keywords: Optional[KeywordDict] = None) -> None:
        """Adds a task to the flow graph

        Parameters
        ----------
        current_task : PipelineTask
            The task that is being added.
        prior_tasks : Optional[List[PipelineTask]]
            Tasks that directly preceed the `current_task` in execution
        keywords : Optional[KeywordDict]
            Mapping that dictates which of the `prior_tasks` are used as keywords input to the `current_task`

        Returns
        -------
        None
        """
        assert isinstance(current_task, PipelineTask), "type doesn't match"

        # If they're none just fill them in with defaults
        if prior_tasks is None:
            prior_tasks = []

        if keywords is None:
            keywords = dict()
        # Make sure that all the keywords are present
        # FlowGraph._expand_keywords(prior_tasks, keywords)
        # assert FlowGraph._validate_keyword_prior_task_matches(prior_tasks, keywords), "keywords don't match tasks"

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
        """ makes a graphical representation of the SegmentGraph"""
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
        return len(self._tasks)

    def iterate_tasks(self) -> PipelineTask:
        for task in self._tasks:
            yield task

    def iterate_edges(self) -> Tuple[PipelineTask, PipelineTask]:
        for start_task in self._adj_list:
            for end_task in self._adj_list[start_task]:
                yield start_task, end_task

    def get_downstream_tasks(self, task: PipelineTask) -> List[PipelineTask]:
        return self._adj_list[task] if task in self._adj_list else []

    def get_upstream_tasks(self, task: PipelineTask) -> List[PipelineTask]:
        pass

    def gather_keywords_into(self, task: PipelineTask) -> Dict[str, PipelineTask]:
        output: Dict[str, PipelineTask] = dict()
        for (start_task, end_task), keyword in self._keywords.items():
            if task == end_task:
                if keyword:
                    output[keyword] = start_task
        return output


class FlowBuilder(metaclass=ABCMeta):
    def __init__(self, database_credentials: DatabaseCredentials, flow_name: str, frequency_in_minutes: Optional[int] = None):
        self.database_credentials = database_credentials
        self.flow_name = flow_name
        self.frequency_in_minutes = frequency_in_minutes

    @abstractmethod
    def build(self) -> Flow:
        pass


class CoreFlowBuilder(FlowBuilder):
    def __init__(self, database_credentials: DatabaseCredentials, level: int, flow_graph: FlowGraph):
        super().__init__(database_credentials, f"core level {level}")
        self.flow_graph = flow_graph
        self.level = level

    def build(self) -> Flow:
        flow = Flow(self.flow_name)
        for task in self.flow_graph.iterate_tasks():
            flow.add_task(task)

        for start_task in self.flow_graph.iterate_tasks():
            flow.set_dependencies(start_task,
                                  keyword_tasks=self.flow_graph.gather_keywords_into(start_task),
                                  downstream_tasks=self.flow_graph.get_downstream_tasks(start_task))

        return flow


class LauncherFlowBuilder(FlowBuilder):
    def __init__(self, database_credentials: DatabaseCredentials, frequency_in_minutes: int):
        super().__init__(database_credentials, "launcher")
        self.frequency_in_minutes = frequency_in_minutes

    def build(self) -> Flow:
        schedule = IntervalSchedule(interval=timedelta(minutes=self.frequency_in_minutes))

        flow = Flow(self.flow_name, schedule=schedule)
        gather_queued_flows = GatherQueuedFlows(user=self.database_credentials.user,
                                                password=self.database_credentials.password,
                                                db_name=self.database_credentials.project_name)
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
    def __init__(self, database_credentials: DatabaseCredentials,
                 level: int, frequency_in_minutes: int, inputs_check_task: CheckForInputs):
        super().__init__(database_credentials, f"scheduler level {level}")
        self.level = level
        self.frequency_in_minutes = frequency_in_minutes
        self.inputs_check_task = inputs_check_task()

    def build(self) -> Flow:
        schedule_flow = ScheduleFlow()

        schedule = IntervalSchedule(interval=timedelta(minutes=self.frequency_in_minutes))
        flow = Flow(self.flow_name, schedule=schedule)
        flow.add_task(self.inputs_check_task)
        # flow.add_task(schedule_flow)
        # flow.set_dependencies(self.inputs_check_task,
        #                       downstream_tasks=[schedule_flow])
        # flow.set_dependencies(schedule_flow,
        #                       keyword_tasks=dict(flow_entry=[self.inputs_check_task]),
        #                       mapped=True)
        return flow


class ProcessFlowBuilder(FlowBuilder):
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
        mark_as_running = MarkFlowAsRunning()
        mark_flow_start_time = MarkFlowStartTime()

        process_flow.set_dependencies(
            rename_flow,
            downstream_tasks=[mark_as_running],
            keyword_tasks=dict(flow_run_name=str(datetime.now()))
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
        mark_as_ended = MarkFlowAsEnded()
        mark_flow_end_time = MarkFlowEndTime()
        create_database_entry_tasks = [CreateFileDatabaseEntry() for _ in self.output_tasks]

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

        for database_task, output_task in zip(create_database_entry_tasks, self.output_tasks):
            print(database_task, output_task)
            process_flow.set_dependencies(
                database_task,
                upstream_tasks=[output_task],
                keyword_tasks=dict(flow_id=flow_id, meta_data=output_task)
            )

        return process_flow

