import asyncio
import sys

from gql import Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.exceptions import TransportQueryError, TransportServerError
from graphql import DocumentNode
from typing import Dict

from commiters_data import CommitersData
from constants import REPOS_QUERY, COMMITS_QUERY, ORGANIZATION, TOKEN, GRAPHQL_URL
from stat_processor import process_stat


async def try_get_response(client: Client, query: DocumentNode, variables: Dict, sem: asyncio.Semaphore):
    try:
        async with sem:
            return await client.execute_async(query, variable_values=variables)
    except TransportQueryError:
        sys.exit('Rate limit exceeded, try again later or change TOKEN')
    except TransportServerError:
        print('Слишком много одновременных запросов! Уменьшите значение asincio.Semaphore!')
        return await try_get_response(client, query, variables, sem)
    except Exception as e:
        print(f'Ошибка {e} на запросе {variables}, повтор запроса')
        return await try_get_response(client, query, variables, sem)


async def process_repo(repo_name, commiters_data, sem: asyncio.Semaphore) -> None:
    tasks = []
    client = get_client()
    query_variables = {"repo": repo_name, "owner": ORGANIZATION}

    while True:
        response = await try_get_response(client, COMMITS_QUERY, query_variables, sem)
        commits_from_response = response["repository"]["defaultBranchRef"]["target"]["history"]

        for commit in commits_from_response["nodes"]:
            if commit["message"].startswith("Merge pull request #"):
                continue
            tasks.append(asyncio.create_task(commiters_data.update(commit)))

        if not commits_from_response["pageInfo"]["hasNextPage"]:
            break

        query_variables["cursor"] = commits_from_response["pageInfo"]["endCursor"]

    await asyncio.gather(*tasks)
    await client.close_async()
    print(f'Обработан репозиторий {repo_name}')


async def get_commiters_data() -> CommitersData:
    commiters_data = CommitersData()
    cursor = None
    client = get_client()
    sem = asyncio.Semaphore(8)
    tasks = []

    while True:
        repos_data = await try_get_response(client, REPOS_QUERY, {"cursor": cursor, "org": ORGANIZATION}, sem)

        for repo in repos_data["organization"]["repositories"]["nodes"]:
            tasks.append(asyncio.create_task(process_repo(repo["name"], commiters_data, sem)))

        if not repos_data["organization"]["repositories"]["pageInfo"]["hasNextPage"]:
            break

        cursor = repos_data["organization"]["repositories"]["pageInfo"]["endCursor"]

    await asyncio.gather(*tasks)
    await client.close_async()
    return commiters_data


def get_client():
    return Client(transport=AIOHTTPTransport(url=GRAPHQL_URL, headers={"Authorization": f"Bearer {TOKEN}"}))


async def main():
    commiters_data = await get_commiters_data()
    top_100_commiters = commiters_data.get_top_100()
    process_stat(top_100_commiters)


if __name__ == '__main__':
    asyncio.run(main())
