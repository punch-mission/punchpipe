from prefect import Flow
import punchpipe.level1.tasks as level1_tasks
from punchpipe.level1.flow import level1_core_flow

# level1_graph = SegmentGraph(1, "Level0 to Level1", None)
# level1_graph.add_task(level1_tasks.destreak_task, None)
#
# level1_core_flow = CoreFlow.initialize("Level 0 to Level 1 core", level1_graph)
# level1_process_flow = level1_core_flow.generate_process_flow()
# level1_core_flow.run()


def test_check_core_flow():
    assert isinstance(level1_core_flow, Flow)
