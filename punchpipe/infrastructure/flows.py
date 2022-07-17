# from __future__ import annotations
# from abc import ABCMeta, abstractmethod
# import prefect
# from prefect import Flow, Parameter
# from prefect.tasks.mysql import MySQLFetch
# from prefect.tasks.prefect.flow_run_rename import RenameFlowRun
# from prefect.schedules import IntervalSchedule
# from prefect.triggers import any_failed
# from typing import Optional, List, Dict, Set, Tuple, NewType
# import graphviz
# from datetime import timedelta, datetime
# from punchpipe.infrastructure.controlsegment import DatabaseCredentials, DatabaseCredentials
# from punchpipe.infrastructure.tasks.core import PipelineTask, OutputTask
# from punchpipe.infrastructure.tasks.processor import MarkFlowAsRunning, MarkFlowAsEnded, MarkFlowStartTime, MarkFlowEndTime, \
#     CreateFileDatabaseEntry, MarkFlowAsFailed
# from punchpipe.infrastructure.tasks.launcher import GatherQueuedFlows, CountRunningFlows, \
#     EscalateLongWaitingFlows, FilterForLaunchableFlows, LaunchFlow
# from punchpipe.infrastructure.tasks.scheduler import CheckForInputs, ScheduleFlow, AdvanceFiles


# __all__ = ['FlowGraph',
#            'FlowBuilder',
#            'CoreFlowBuilder',
#            'LauncherFlowBuilder',
#            'SchedulerFlowBuilder',
#            'ProcessFlowBuilder',
#            'KeywordDict']

# KeywordDict = NewType("KeywordDict", Dict[Tuple[PipelineTask, PipelineTask], Optional[str]])


# class LauncherFlowBuilder(FlowBuilder):
#     """A flow builder that makes launcher flows, i.e. the flows that start process flows running.
#     """
#     def __init__(self, database_credentials: DatabaseCredentials, frequency_in_minutes: int):
#         super().__init__(database_credentials, "launcher")
#         self.frequency_in_minutes = frequency_in_minutes

#     def build(self) -> Flow:
#         # Prepare the schedule
#         schedule = IntervalSchedule(interval=timedelta(minutes=self.frequency_in_minutes))

#         flow = Flow(self.flow_name, schedule=schedule)

#         # Create all necessary tasks and add them to the flow
#         gather_queued_flows = GatherQueuedFlows(user=self.database_credentials.user,
#                                                 password=self.database_credentials.password,
#                                                 db_name=self.database_credentials.project_name,
#                                                 fetch='all')
#         count_running_flows = CountRunningFlows(user=self.database_credentials.user,
#                                                 password=self.database_credentials.password,
#                                                 db_name=self.database_credentials.project_name)
#         escalate_long_waiting_flows = EscalateLongWaitingFlows(user=self.database_credentials.user,
#                                                                password=self.database_credentials.password,
#                                                                db_name=self.database_credentials.project_name)
#         filter_for_launchable_flows = FilterForLaunchableFlows()
#         launch_flow = LaunchFlow()

#         flow.add_task(gather_queued_flows)
#         flow.add_task(count_running_flows)
#         flow.add_task(escalate_long_waiting_flows)
#         flow.add_task(filter_for_launchable_flows)
#         flow.add_task(launch_flow)

#         # Build the dependencies between tasks
#         flow.set_dependencies(count_running_flows,
#                               downstream_tasks=[filter_for_launchable_flows])
#         flow.set_dependencies(escalate_long_waiting_flows,
#                               downstream_tasks=[gather_queued_flows])
#         flow.set_dependencies(gather_queued_flows,
#                               downstream_tasks=[filter_for_launchable_flows])
#         flow.set_dependencies(filter_for_launchable_flows,
#                               downstream_tasks=[launch_flow],
#                               keyword_tasks=dict(running_flow_count=count_running_flows,
#                                                  priority_sorted_queued_flows=gather_queued_flows))
#         flow.set_dependencies(launch_flow,
#                               downstream_tasks=[],
#                               keyword_tasks=dict(flow_entry=filter_for_launchable_flows),
#                               mapped=True)
#         return flow


# class SchedulerFlowBuilder(FlowBuilder):
#     """A flow builder that creates scheduler flows, i.e. the kinds of flows that look for ready files and schedule
#     process flows to process them.
#     """
#     def __init__(self, database_credentials: DatabaseCredentials,
#                  level: int, frequency_in_minutes: int, inputs_check_task: CheckForInputs, query_task: MySQLFetch):
#         super().__init__(database_credentials, f"scheduler level {level}")
#         self.level = level
#         self.frequency_in_minutes = frequency_in_minutes
#         self.query_task = query_task(user=self.database_credentials.user,
#                                      password=self.database_credentials.password,
#                                      db_name=self.database_credentials.project_name,
#                                      fetch='all')
#         self.inputs_check_task = inputs_check_task()

#     def build(self) -> Flow:
#         logger = prefect.context.get("logger")

#         schedule_flow = ScheduleFlow(user=self.database_credentials.user,
#                                      password=self.database_credentials.password,
#                                      db_name=self.database_credentials.project_name,
#                                      commit=True)
#         advance_files = AdvanceFiles(user=self.database_credentials.user,
#                                      password=self.database_credentials.password,
#                                      db_name=self.database_credentials.project_name,
#                                      commit=True)

#         schedule = IntervalSchedule(interval=timedelta(minutes=self.frequency_in_minutes))
#         flow = Flow(self.flow_name, schedule=schedule)
#         flow.add_task(self.query_task)
#         flow.add_task(self.inputs_check_task)
#         flow.add_task(schedule_flow)
#         flow.add_task(advance_files)

#         flow.set_dependencies(self.query_task,
#                               downstream_tasks=[self.inputs_check_task])
#         flow.set_dependencies(self.inputs_check_task,
#                               downstream_tasks=[schedule_flow],
#                               keyword_tasks=dict(query_result=self.query_task))
#         flow.set_dependencies(advance_files,
#                               downstream_tasks=[],
#                               keyword_tasks=dict(query_result=self.query_task))
#         flow.set_dependencies(schedule_flow,
#                               keyword_tasks=dict(pair=self.inputs_check_task),
#                               mapped=True)
#         return flow


# class ProcessFlowBuilder(FlowBuilder):
#     """A flow builder that converts core flows into process flows.
#     """
#     def __init__(self, database_credentials: DatabaseCredentials, level: int, core_flow: Flow) -> None:
#         super().__init__(database_credentials, f"processor level {level}")
#         self.core_flow = core_flow
#         self.level = level

#         self.core_flow_parameters = self.core_flow.parameters()

#         # Construct the entry tasks.
#         # A task is an entry task if it is a non-Parameter root task, i.e. a task with no upstream tasks that is also
#         # not a parameter, or if it is a Parameter's direct downstream task.
#         self.entry_tasks = set()
#         for task in self.core_flow.root_tasks():
#             if isinstance(task, Parameter):
#                 self.entry_tasks = self.entry_tasks.union(self.core_flow.downstream_tasks(task))
#             else:
#                 self.entry_tasks.add(task)

#         # Construct ending tasks. These are simply the terminal tasks of the core flow.
#         self.ending_tasks = self.core_flow.terminal_tasks()

#         # Tasks that are output tasks
#         self.output_tasks = self.core_flow.get_tasks(task_type=OutputTask)

#     def build(self) -> Flow:
#         process_flow = self.core_flow.copy()
#         process_flow.name = process_flow.name.replace("core", "process")
#         flow_id = Parameter("flow_id")

#         # set up the pre-flow tasks
#         rename_flow = RenameFlowRun()
#         mark_as_running = MarkFlowAsRunning(user=self.database_credentials.user,
#                                      password=self.database_credentials.password,
#                                      db_name=self.database_credentials.project_name,
#                                      commit=True)
#         mark_flow_start_time = MarkFlowStartTime(user=self.database_credentials.user,
#                                      password=self.database_credentials.password,
#                                      db_name=self.database_credentials.project_name,
#                                      commit=True)

#         process_flow.set_dependencies(
#             rename_flow,
#             downstream_tasks=[mark_as_running],
#             keyword_tasks=dict(flow_run_name=flow_id)
#         )
#         process_flow.set_dependencies(
#             mark_as_running,
#             downstream_tasks=[mark_flow_start_time],
#             keyword_tasks=dict(flow_id=flow_id)
#         )
#         process_flow.set_dependencies(
#             mark_flow_start_time,
#             downstream_tasks=self.entry_tasks,
#             keyword_tasks=dict(flow_id=flow_id)
#         )

#         # set up the post-flow tasks
#         mark_as_ended = MarkFlowAsEnded(user=self.database_credentials.user,
#                                      password=self.database_credentials.password,
#                                      db_name=self.database_credentials.project_name,
#                                      commit=True)
#         mark_flow_end_time = MarkFlowEndTime(user=self.database_credentials.user,
#                                      password=self.database_credentials.password,
#                                      db_name=self.database_credentials.project_name,
#                                      commit=True)
#         create_database_entry_tasks = [CreateFileDatabaseEntry(user=self.database_credentials.user,
#                                                                password=self.database_credentials.password,
#                                                                db_name=self.database_credentials.project_name,
#                                                                commit=True) for _ in self.output_tasks]

#         process_flow.set_dependencies(
#             mark_flow_end_time,
#             upstream_tasks=self.ending_tasks,
#             keyword_tasks=dict(flow_id=flow_id)
#         )

#         process_flow.set_dependencies(
#             mark_as_ended,
#             upstream_tasks=[mark_flow_end_time],
#             keyword_tasks=dict(flow_id=flow_id)
#         )

#         for database_task, output_task in zip(create_database_entry_tasks, self.output_tasks):
#             process_flow.set_dependencies(
#                 database_task,
#                 upstream_tasks=[output_task],
#                 keyword_tasks=dict(meta_data=output_task, flow_id=flow_id)
#             )

#         # Set up the graceful exit if something went wrong in executing the flow
#         # We add a task that follows all core flow and database tasks
#         # and marks the flow as failed if any of the prior tasks fail
#         # It does require the flow_id to know which flow failed
#         existing_tasks = process_flow.get_tasks()
#         core_failure_task = MarkFlowAsFailed(trigger=any_failed, user=self.database_credentials.user,
#                                      password=self.database_credentials.password,
#                                      db_name=self.database_credentials.project_name,
#                                      commit=True)
#         process_flow.add_task(core_failure_task)
#         process_flow.set_dependencies(core_failure_task, upstream_tasks=existing_tasks,
#                                       keyword_tasks=dict(flow_id=flow_id))

#         # The process flow is successful or not depending on the status of the core tasks
#         process_flow.set_reference_tasks(self.core_flow.get_tasks())

#         return process_flow

