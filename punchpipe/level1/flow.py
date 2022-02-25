from punchpipe.infrastructure.flows import FlowGraph, CoreFlowBuilder, ProcessFlowBuilder
from punchpipe.level1.tasks import destreak_task
from punchpipe.infrastructure.controlsegment import DatabaseCredentials

db_cred = DatabaseCredentials("project_name", "user", "password")

level1_graph: FlowGraph = FlowGraph(1, "Level0 to Level1")

level1_graph.add_task(destreak_task, None)

level1_core_flow = CoreFlowBuilder(db_cred, 1, level1_graph).build()
level1_process_flow = ProcessFlowBuilder(db_cred, 1, level1_core_flow).build()
