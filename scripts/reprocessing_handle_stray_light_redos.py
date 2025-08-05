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

from punchpipe.control.db import File, Flow
from punchpipe.control.util import get_database_session

root_dirs = sys.argv[1:]

session = get_database_session()

did_reset = False
for type in ['M', 'Z', 'P', 'R']:
    for observatory in ['1', '2', '3', '4']:
        n_models = (session.query(File)
                           .where(File.file_type == 'S' + type)
                           .where(File.observatory == observatory)
                           .where(File.state == 'created')
                           .count())
        if n_models < 2:
            print(f"*{type}{observatory} ⏳ : waiting on stray light models")
            continue

        x_files = (session.query(File)
                          .where(File.file_type == 'X' + type)
                          .where(File.observatory == observatory)
                          .where(File.level == '1')
                          .all())
        x_files = {f.date_obs: f for f in x_files}
        q_files = (session.query(File)
                          .where(File.file_type == 'Q' + type)
                          .where(File.observatory == observatory)
                          .where(File.level == '1')
                          .all())
        q_files = {f.date_obs: f for f in q_files}

        xs_without_qs = x_files.keys() - q_files.keys()
        if len(xs_without_qs):
            did_reset = True
            ids_to_reset = []
            for dateobs in xs_without_qs:
                file = x_files[dateobs]
                ids_to_reset.append(file.processing_flow)
            flows_to_reset = (session.query(Flow)
                              .where(Flow.flow_id.in_(ids_to_reset))
                              .where(Flow.state != 'revivable')
                              .all())
            for flow in flows_to_reset:
                flow.state = 'revivable'
            print(f"*{type}{observatory} 🔄 : reset {len(flows_to_reset)} L1-early flows")
        else:
            print(f"*{type}{observatory} ✅ : looks good!")
session.commit()

if did_reset:
    print("Any running or planned stray light models may crash as we're removing their input X files, but that's OK. "
          "They'll re-schedule when their inputs are regenerated")