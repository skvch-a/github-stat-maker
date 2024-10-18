import asyncio
import sys

from gql import Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.exceptions import TransportQueryError, TransportServerError
from graphql import DocumentNode
from typing import List, Dict

from commiters_data import CommitersData
from constants import REPOS_QUERY, BRANCHES_QUERY, COMMITS_QUERY, ORGANIZATION, TOKEN, GRAPHQL_URL
from processed_commits import ProcessedCommits
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


async def get_branch_names(repo_name: str, sem: asyncio.Semaphore) -> List[str]:
    branch_names = []
    client = get_client()
    query_variables = {"repo": repo_name, "owner": ORGANIZATION}

    while True:
        response = await try_get_response(client, BRANCHES_QUERY, query_variables, sem)

        if response is None:
            break

        branches = response["repository"]["refs"]
        branch_names.extend(branch["name"] for branch in branches["nodes"])

        if not branches["pageInfo"]["hasNextPage"]:
            break

        query_variables["cursor"] = branches["pageInfo"]["endCursor"]

    await client.close_async()
    return branch_names


async def process_commits(repo_name, branch_name, processed_commits, commiters_data, sem: asyncio.Semaphore) -> None:
    tasks = []
    is_commits_already_processed = False
    client = get_client()
    query_variables = {"repo": repo_name, "branch": "refs/heads/" + branch_name, "owner": ORGANIZATION}

    while True:
        response = await try_get_response(client, COMMITS_QUERY, query_variables, sem)

        if response is None:
            break

        commits_from_response = response["repository"]["ref"]["target"]["history"]
        for commit in commits_from_response["nodes"]:
            if await  processed_commits.contains(commit["oid"]):
                is_commits_already_processed = True
                break
            if commit["message"].startswith("Merge pull request #"):
                continue
            await processed_commits.add(commit["oid"])
            tasks.append(asyncio.create_task(commiters_data.update(commit)))

        if not commits_from_response["pageInfo"]["hasNextPage"] or is_commits_already_processed:
            break

        query_variables["cursor"] = commits_from_response["pageInfo"]["endCursor"]

    await asyncio.gather(*tasks)
    await client.close_async()


async def get_commiters_data() -> CommitersData:
    commiters_data = CommitersData()
    cursor = None
    repo_count = 1
    client = get_client()
    sem = asyncio.Semaphore(10)
    tasks = []

    while True:
        repos_data = await try_get_response(client, REPOS_QUERY, {"cursor": cursor, "org": ORGANIZATION}, sem)

        if repos_data is None:
            break

        for repo in repos_data["organization"]["repositories"]["nodes"]:
            print(f'Обрабатывается репозиторий: {repo_count} - {repo["name"]}')
            repo_count += 1
            processed_commits = ProcessedCommits()
            repo_name = repo["name"]
            for branch_name in await get_branch_names(repo_name, sem):
                tasks.append(asyncio.create_task(
                    process_commits(repo_name, branch_name, processed_commits, commiters_data, sem)))

        if not repos_data["organization"]["repositories"]["pageInfo"]["hasNextPage"]:
            break

        cursor = repos_data["organization"]["repositories"]["pageInfo"]["endCursor"]

    print('Завершение обработки...')
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
