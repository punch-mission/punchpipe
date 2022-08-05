from punchpipe.controlsegment.db import Flow, MySQLCredentials
from punchpipe.flows.level1 import level1_process_flow

from prefect import task, flow
from prefect.client import get_client
from datetime import datetime, timedelta
from sqlalchemy import and_, create_engine
from sqlalchemy.orm import Session
from typing import List, Any
import json


@task
def gather_queued_flows(session):
    return [f.flow_id for f in session.query(Flow).where(Flow.state == "queued").all()]


@task
def count_running_flows(session):
    return len(session.query(Flow).where(Flow.state == "running").all())


@task
def escalate_long_waiting_flows(session, max_seconds_waiting=100, escalated_priority=1):
    since = datetime.now() - timedelta(seconds=max_seconds_waiting)
    session.query(Flow).where(and_(Flow.state == "queued", Flow.creation_time < since)).update(
        {'priority': escalated_priority})
    session.commit()


@task
def filter_for_launchable_flows(queued_flows, running_flow_count, max_flows_running: int = 10):
    number_to_launch = max_flows_running - running_flow_count
    if number_to_launch > 0:
        if queued_flows:  # there are flows to run
            return queued_flows[:number_to_launch]
        else:
            return []
    else:
        return []


@task
async def launch_ready_flows(session: Session, flow_ids: List[int]):
    """Given a list of ready-to-launch flow_ids, this task creates flow runs in Prefect for them.
    These flow runs are automatically marked as scheduled in Prefect and will be picked up by a work queue and
    agent as soon as possible.

    Parameters
    ----------
    session : sqlalchemy.orm.session.Session
        A SQLAlchemy session for database interactions
    flow_ids : List[int]
        A list of flow IDs from the punchpipe database identifying which flows to launch

    Returns
    -------
    A list of responses from Prefect about the flow runs that were created
    """
    # gather the flow information for launching
    flow_info = session.query(Flow).where(Flow.flow_id in flow_ids).all()

    responses = []
    async with get_client() as client:
        # determine the deployment ids for each kind of flow
        deployments = await client.read_deployments()
        deployment_ids = {d.name: d.id for d in deployments}

        # for every flow launch it and store the response
        for this_flow in flow_info:
            this_deployment_id = deployment_ids[this_flow.name]
            this_flow_params = json.loads(this_flow.call_data)
            response = await client.create_flow_run_from_deployment(this_deployment_id, parameters=this_flow_params)
            responses.append(response)
    return responses


@flow
def launcher_flow():
    """The main launcher flow for Prefect, responsible for identifying flows, based on priority,
        that are ready to run and creating flow runs for them. It also escalates long-waiting flows' priorities.

    See EM 41 or the internal requirements document for more details

    Returns
    -------
    Nothing
    """
    # Get database credentials and establish a connection
    credentials = MySQLCredentials.load('mysql-cred')
    engine = create_engine(
        f'mysql+pymysql://{credentials.user}:{credentials.password.get_secret_value()}@localhost/punchpipe')
    session = Session(engine)

    # Perform the launcher flow responsibilities
    num_running_flows = count_running_flows(session)
    escalate_long_waiting_flows(session)
    queued_flows = gather_queued_flows(session)
    flows_to_launch = filter_for_launchable_flows(queued_flows, num_running_flows)
    launch_ready_flows(session, flows_to_launch)
