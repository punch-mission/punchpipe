from __future__ import annotations
import os
from pytest import fixture
from prefect import Flow
from flows import FlowGraph, CoreFlowBuilder, KeywordDict, ProcessFlowBuilder
from tasks import PipelineTask
from controlsegment import DatabaseCredentials


@fixture
def default_database_credentials():
    return DatabaseCredentials("test", "user", "password")


@fixture
def empty_segment_graph():
    return FlowGraph(5, "test", None)


@fixture
def initialized_segment_graph():
    tasks = [PipelineTask(str(i)) for i in range(3)]
    adj_list = {None: [tasks[0]],
                tasks[0]: [tasks[1], tasks[2]]}
    keywords: KeywordDict = KeywordDict({(tasks[0], tasks[1]): "dummy"})
    return FlowGraph(0, "test", adj_list=adj_list, keywords=keywords)


@fixture
def expanded_segment_graph():
    tasks = [PipelineTask(str(i)) for i in range(3)]
    adj_list = {None: [tasks[0]],
                tasks[0]: [tasks[1], tasks[2]],
                tasks[1]: [],
                tasks[2]: []}
    keywords: KeywordDict = KeywordDict({(tasks[0], tasks[1]): "dummy"})
    f = FlowGraph(0, "test", adj_list=adj_list, keywords=keywords)
    new_task1 = PipelineTask("4")
    f.add_task(new_task1, [tasks[2]], KeywordDict({(new_task1, tasks[2]): "new edge"}))
    new_task2 = PipelineTask("5")
    f.add_task(new_task2, [new_task1], None)
    return f


@fixture
def empty_flow(empty_segment_graph):
    return CoreFlowBuilder(0, empty_segment_graph).build()


def test_segment_graph_init(empty_segment_graph):
    assert isinstance(empty_segment_graph, FlowGraph), "SegmentGraph isn't even the right type"
    assert len(empty_segment_graph) == 0, "Task list not created as empty properly"


def test_segment_graph_add_tasks(empty_segment_graph):
    my_first_task = PipelineTask(1)
    empty_segment_graph.add_task(my_first_task)
    assert len(empty_segment_graph) == 1, "Task was not added properly"
    my_second_task = PipelineTask(2)
    empty_segment_graph.add_task(my_second_task)
    assert len(empty_segment_graph) == 2, "Task was not added properly"


def test_segment_graph_keywords_at_initialization(initialized_segment_graph):
    assert len(initialized_segment_graph._keywords) == 1


def test_segment_graph_keywords_at_expansion(expanded_segment_graph):
    assert len(expanded_segment_graph._keywords) == 2


def test_flow_generation(empty_flow):
    assert isinstance(empty_flow, Flow)


def test_process_flow_conversion(empty_flow, default_database_credentials):
    process_flow = ProcessFlowBuilder(default_database_credentials, 0, empty_flow).build()
    assert isinstance(process_flow, Flow)


def test_segment_graph_render(initialized_segment_graph):
    initialized_segment_graph.render("test")
    assert os.path.exists("test.pdf")
    assert os.path.exists("test")
    if os.path.exists("test.pdf"):
        os.remove("test.pdf")
    if os.path.exists("test"):
        os.remove("test")


def test_flow_convert_core_to_process(initialized_segment_graph, default_database_credentials):
    core_flow = CoreFlowBuilder(0, initialized_segment_graph).build()
    process_flow = ProcessFlowBuilder(default_database_credentials, 0, core_flow).build()
    process_flow.visualize()
