"""
When reprocessing is started, at first there are no stray light models. The L1-early flows processed during that
time will produce X files but no Q files. As models start to be generated, this script should be run to reset those
flows so the stray light-subtracted Q files can be generated. For each observatory/polarizer combo, this script will
report whether (1) we're still waiting for 2 stray light models, (2) We have >= 2 stray light models,
and some L1-early flows that produced X files but no Q files were found and reset, or (3) We have >= 2 stray light
models, and all X files have a corresponding Q file. Only when all polarizer/observatory combos report this third
state should the L1-late flow be enabled.
"""
import sys
from datetime import timedelta

from punchpipe.control.db import File, Flow
from punchpipe.control.util import get_database_session

root_dirs = sys.argv[1:]

session = get_database_session()

did_reset = False
for type in ['M', 'Z', 'P', 'R']:
    for observatory in ['1', '2', '3', '4']:
        stray_light_models = (session.query(File)
                              .where(File.file_type == 'S' + type)
                              .where(File.observatory == observatory)
                              .where(File.state == 'created')
                              .all())
        if len(stray_light_models) < 2:
            print(f"*{type}{observatory} ⏳ : waiting on stray light models")
            continue

        creation_times = [model.date_created for model in stray_light_models]
        cutoff_time = sorted(creation_times)[1] + timedelta(minutes=1)

        early_flows = (session.query(Flow)
                       .join(File, File.processing_flow == Flow.flow_id)
                       .where(File.file_type == 'X' + type)
                       .where(File.observatory == observatory)
                       .where(Flow.flow_type == 'level1_early')
                       .where(Flow.creation_time < cutoff_time)
                       .where(Flow.state != 'revivable')
                       .all())

        if len(early_flows):
            did_reset = True
            for flow in early_flows:
                flow.state = 'revivable'
            session.commit()
            print(f"*{type}{observatory} 🔄 : reset {len(early_flows)} L1-early flows")
        else:
            print(f"*{type}{observatory} ✅ : looks good!")

if did_reset:
    print("Any running or planned stray light models may crash as we're removing their input X files, but that's OK. "
          "They'll re-schedule when their inputs are regenerated")
