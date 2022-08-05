import asyncio
from prefect.client import get_client
from prefect import flow
from punchpipe.flows.level1 import level1_process_flow


async def main():
    async with get_client() as client:
        # depl_id = "bf47fe9f-cc0c-4625-83dc-b1023c81e6a6"
        deployments = await client.read_deployments()
        print(deployments)
        my_org = {d.name: d.id for d in deployments}
        print(my_org)
        depl_id = my_org['level1-process-flow']
        response1 = await client.create_flow_run_from_deployment(depl_id)
        response2 = await client.create_flow_run_from_deployment(depl_id)
        response3 = await client.create_flow_run_from_deployment(depl_id)
        response4 = await client.create_flow_run_from_deployment(depl_id)
        print(response1)
        print(response2)
        print(response3)
        print(response4)


async def main2():
    async with get_client() as client:
        outcome = await client.create_flow_run(test_flow)
        print(outcome)


@flow
async def test_flow():
    await asyncio.sleep(30)


@flow
async def main_flow():
    parallel_subflows = [test_flow(), test_flow(), test_flow()]
    await asyncio.gather(*parallel_subflows)

if __name__ == "__main__":
    asyncio.run(main())
    # main_flow_state = asyncio.run(main_flow())
