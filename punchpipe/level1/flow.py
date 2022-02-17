from controlsegment.flows import FlowGraph, FlowBuilder, ProcessFlowGenerator
from tasks import destreak_task

level1_graph: FlowGraph = FlowGraph(1, "Level0 to Level1", None)

level1_graph.add_task(destreak_task, None)

level1_core_flow = FlowBuilder("Level 0 to Level 1 core", level1_graph).build()
level1_process_flow = ProcessFlowGenerator(level1_core_flow).generate()
