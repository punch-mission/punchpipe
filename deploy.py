from prefect import serve
from punchpipe.controlsegment.launcher import launcher_flow
from punchpipe.flows.level1 import level1_process_flow, level1_scheduler_flow
from punchpipe.flows.level2 import level2_process_flow, level2_scheduler_flow
from punchpipe.flows.level3 import level3_process_flow, level3_scheduler_flow

launcher_deployment = launcher_flow.to_deployment(name="launcher-deployment",
                                                    description="Launch a pipeline segment.",
                                                    cron="* * * * *",
                                                    )

level1_scheduler_deployment = level1_scheduler_flow.to_deployment(name="level1-scheduler-deployment",
                    description="Schedule a Level 1 flow.",
                    cron="* * * * *",
                    )
level1_process_deployment = level1_process_flow.to_deployment(name="level1_process_flow",
                                                              description="Process a file from Level 0 to Level 1.")

level2_scheduler_deployment = level2_scheduler_flow.to_deployment(name="level2-scheduler-deployment",
                    description="Schedule a Level 2 flow.",
                    cron="* * * * *",
                    )
level2_process_deployment = level2_process_flow.to_deployment(name="level2_process_flow",
                                                              description="Process files from Level 1 to Level 2.")

# level3_scheduler_deployment = level3_scheduler_flow.to_deployment(name="level3-scheduler-deployment",
#                     description="Schedule a Level 3 flow.",
#                     cron="* * * * *",
#                     )
# level3_process_deployment = level3_process_flow.to_deployment(name="level3_process_flow",
#                                                               description="Process files from Level 2 to Level 3.")


serve(launcher_deployment,
      level1_scheduler_deployment, level1_process_deployment,
      level2_scheduler_deployment, level2_process_deployment,
      # level3_scheduler_deployment, level3_process_deployment
      )
