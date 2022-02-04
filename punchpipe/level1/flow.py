from controlsegment.flows import CoreFlow, SegmentGraph
from tasks import destreak_task

level1_graph: SegmentGraph = SegmentGraph(1, "Level0 to Level1")

level1_graph.add_task(destreak_task, None)

level1_core_flow = CoreFlow.initialize(level1_graph)
level1_process_flow = level1_core_flow.generate_process_flow()