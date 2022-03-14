from punchpipe.infrastructure.flows import FlowGraph, CoreFlowBuilder, ProcessFlowBuilder
from punchpipe.level1.tasks import destreak_task, input_filename, output_filename, load_level1, output_level2
from punchpipe.infrastructure.controlsegment import DatabaseCredentials

db_cred = DatabaseCredentials("project_name", "user", "password")

level1_graph: FlowGraph = FlowGraph(1, "Level0 to Level1")
level1_graph.add_task(input_filename, None)
level1_graph.add_task(output_filename, None)
level1_graph.add_task(load_level1, [input_filename], keywords={input_filename: "input_filename"})
level1_graph.add_task(destreak_task, [load_level1], keywords={load_level1: "data_object"})
level1_graph.add_task(output_level2, [destreak_task, output_filename],
                      keywords={destreak_task: "data", output_filename: "path"})

level1_core_flow = CoreFlowBuilder(db_cred, 1, level1_graph).build()  # TODO: core flow builder shouldn't need db_cred
