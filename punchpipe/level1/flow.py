from punchpipe.infrastructure.flows import FlowGraph, CoreFlowBuilder, ProcessFlowBuilder
from punchpipe.level1.tasks import destreak_task, input_filename, output_filename, load_level1, output_level2, \
    align_task, deficient_pixel_removal_task, despike_task, flagging_task, psf_correction_task,\
    quartic_fit_task, vignetting_correction_task, stray_light_removal_task

level1_graph: FlowGraph = FlowGraph(1, "Level0 to Level1")
level1_graph.add_task(input_filename, None)
level1_graph.add_task(output_filename, None)
level1_graph.add_task(load_level1, [input_filename], keywords={input_filename: "path"})
level1_graph.add_task(quartic_fit_task, [load_level1], keywords={load_level1: "data_object"})
level1_graph.add_task(despike_task, [quartic_fit_task], keywords={quartic_fit_task: "data_object"})
level1_graph.add_task(destreak_task, [despike_task], keywords={despike_task: "data_object"})
level1_graph.add_task(vignetting_correction_task, [destreak_task], keywords={destreak_task: "data_object"})
level1_graph.add_task(deficient_pixel_removal_task, [vignetting_correction_task],
                      keywords={vignetting_correction_task: "data_object"})
level1_graph.add_task(stray_light_removal_task, [deficient_pixel_removal_task],
                      keywords={deficient_pixel_removal_task: "data_object"})
level1_graph.add_task(align_task, [stray_light_removal_task],
                      keywords={stray_light_removal_task: "data_object"})
level1_graph.add_task(psf_correction_task, [align_task],
                      keywords={align_task: "data_object"})
level1_graph.add_task(flagging_task, [psf_correction_task],
                      keywords={psf_correction_task: "data_object"})
level1_graph.add_task(output_level2, [flagging_task, output_filename],
                      keywords={flagging_task: "data", output_filename: "path"})

level1_core_flow = CoreFlowBuilder(1, level1_graph).build()  # TODO: core flow builder shouldn't need db_cred
