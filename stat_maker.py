import sys
import asyncio

from gql import Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.exceptions import TransportQueryError, TransportServerError

from constants import REPOS_QUERY, BRANCHES_QUERY, COMMITS_QUERY, ORGANIZATION, TOKEN, GRAPHQL_URL
from stat_processor import process_stat
from commiters_data import CommitersData
from processed_commits import ProcessedCommits


async def try_get_response(client, query, variables, sem):
    try:
        async with sem:
            return await client.execute_async(query, variable_values=variables)
    except TransportQueryError:
        sys.exit('Rate limit exceeded, try again later or change TOKEN')
    except TransportServerError:
        print('Слишко много одноврменных запросов! Уменьшите значение asincio.Semaphore!')
        return await try_get_response(client, query, variables, sem)
    except Exception as e:
        print(f'Ошибка {e} на запросе {variables}, повтор запроса')
        return await try_get_response(client, query, variables, sem)


async def get_branch_names(repo_name, sem):
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


async def get_commits_for_branch(repo_name, branch_name, processed_commits, commiters_data, sem):
    all_commits = []
    is_over = False
    client = get_client()
    query_variables = {"repo": repo_name,
                       "branch": "refs/heads/" + branch_name,
                       "owner": ORGANIZATION}

    while True:
        response = await try_get_response(client, COMMITS_QUERY, query_variables, sem)

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

        query_variables["cursor"] = commits["pageInfo"]["endCursor"]

    await client.close_async()
    await commiters_data.update(all_commits)



async def get_commiters_data():
    commiters_data = CommitersData()
    cursor = None
    repo_count = 1
    client = get_client()
    sem = asyncio.Semaphore(40)
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
                task = asyncio.create_task(
                    get_commits_for_branch(repo_name, branch_name, processed_commits, commiters_data, sem))
                tasks.append(task)

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
