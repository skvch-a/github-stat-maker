import sys
import asyncio

from gql import Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.exceptions import TransportQueryError

from constants import REPOS_QUERY, BRANCHES_QUERY, COMMITS_QUERY, ORGANIZATION, TOKEN, GRAPHQL_URL
from stat_processor import process_stat
from commiters_data import CommitersData
from processed_commits import ProcessedCommits

SEM = asyncio.Semaphore(8)

async def try_get_response(client, query, variables):
    try:
        return await client.execute_async(query, variable_values=variables)
    except TransportQueryError:
        sys.exit('Rate limit exceeded, try again later or change TOKEN')
    except Exception as e:
        print(f'Ошибка {e} на запросе {variables}, повтор запроса')
        return await try_get_response(client, query, variables)


async def get_branch_names(repo_name):
    branches_names = []
    cursor = None
    client = get_client()
    while True:

        response = await try_get_response(client,
                                          BRANCHES_QUERY,
                                          {"repo": repo_name, "cursor": cursor, "owner": ORGANIZATION})
        if response is None:
            break
        branches = response["repository"]["refs"]
        branches_names.extend(branch["name"] for branch in branches["nodes"])

        if not branches["pageInfo"]["hasNextPage"]:
            break

        cursor = branches["pageInfo"]["endCursor"]

    await client.close_async()
    return branches_names


async def get_commits_for_branch(repo_name, branch_name, processed_commits, commiters_data):
    async with SEM:
        all_commits = []
        is_over = False
        cursor = None
        client = get_client()

        while True:
            response = await try_get_response(client, COMMITS_QUERY,
                                              {"repo": repo_name,
                                               "branch": "refs/heads/" + branch_name,
                                               "cursor": cursor,
                                               "owner": ORGANIZATION})
            if response is None:
                break

            commits = response["repository"]["ref"]["target"]["history"]
            for commit in commits["nodes"]:
                if await  processed_commits.contains(commit["oid"]):
                    is_over = True
                    break
                if commit["message"].startswith("Merge pull request #"):
                    continue
                await processed_commits.add(commit["oid"])
                all_commits.append(commit)

            if not commits["pageInfo"]["hasNextPage"] or is_over:
                break

            cursor = commits["pageInfo"]["endCursor"]

        await client.close_async()
        await commiters_data.update(all_commits)



async def get_commiters_data():
    commiters_data = CommitersData()
    cursor = None
    repo_count = 1
    client = get_client()

    while True:
        tasks = []
        repos_data = await try_get_response(client, REPOS_QUERY, {"cursor": cursor, "org": ORGANIZATION})

        if repos_data is None:
            break

        for repo in repos_data["organization"]["repositories"]["nodes"]:
            print(f'Обрабатывается репозиторий: {repo_count} - {repo["name"]}')
            repo_count += 1
            processed_commits = ProcessedCommits()
            repo_name = repo["name"]
            for branch_name in await get_branch_names(repo_name):
                task = asyncio.create_task(
                    get_commits_for_branch(repo_name, branch_name, processed_commits, commiters_data))
                tasks.append(task)

        await asyncio.gather(*tasks)

        if not repos_data["organization"]["repositories"]["pageInfo"]["hasNextPage"]:
            break

        cursor = repos_data["organization"]["repositories"]["pageInfo"]["endCursor"]

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
