from punchpipe.controlsegment.db import File

from prefect import task


@task
def update_file_state(session, file_id, new_state):
    session.query(File).where(File.file_id == file_id).update({"state": new_state})
    session.commit()
