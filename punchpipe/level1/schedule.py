

class Level1QueryTask(MySQLFetch):
    """Queries which Level 0 files are ready for the Level 0 to Level 1 processing pipeline"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args,
                         query="SELECT * FROM files WHERE state = 'finished' AND level = 0",
                         **kwargs)


class Level1InputsCheck(CheckForInputs):
    """Converts the Level1QueryTask results to a more useable format for the pipeline

    Specifically, it creates a FlowEntry object and a FileEntry object for passing through the pipeline and scheduling
    """
    def run(self, query_result):
        output = []
        date_format = "%Y%m%dT%H%M%S"
        if query_result is not None:  # None can occur if we did not use "fetch all" for the query task
            for result in query_result:
                # extract information needed for construction FlowEntry and FileEntry
                now = datetime.now()
                now_time_str = datetime.strftime(now, date_format)
                date_acquired = result[6]
                date_obs = result[7]
                incoming_filename = result[-1]

                observation_time_str = datetime.strftime(date_obs, date_format)
                this_flow_id = f"level1_obs{observation_time_str}_run{now_time_str}"

                # construct the flow entry
                new_flow = FlowEntry(
                    flow_type="process level 1",
                    flow_id=this_flow_id,
                    state="queued",
                    creation_time=now,
                    priority=1,
                    call_data=json.dumps({"flow_id": this_flow_id,
                                          'input_filename': f'/Users/jhughes/Desktop/repos/punchpipe/example_run_data/' + incoming_filename,
                                          'output_filename': f'/Users/jhughes/Desktop/repos/punchpipe/example_run_data/'})
                )

                # construct the FileEntry
                new_file = FileEntry(
                    level=2,
                    file_type="XX",
                    observatory="X",
                    file_version=1,
                    software_version=1,
                    date_acquired=date_acquired,
                    date_observation=date_obs,
                    date_end=date_obs,
                    polarization="XX",
                    state="queued",
                    processing_flow=this_flow_id
                )
                output.append((new_flow, new_file))
        return output