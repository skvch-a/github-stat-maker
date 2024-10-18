import sys
import asyncio

from gql import Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.exceptions import TransportQueryError

from constants import REPOS_QUERY, BRANCHES_QUERY, COMMITS_QUERY, ORGANIZATION, TOKEN, GRAPHQL_URL
from stat_processor import process_stat

SEM = asyncio.Semaphore(8)

async def update_authors(authors_by_emails, commits, lock):
    for commit in commits:
        name = commit["author"]["name"]
        email = commit["author"]["email"]

        async with lock:
            if email in authors_by_emails:
                authors_by_emails[email]["commits_count"] += 1
            else:
                authors_by_emails[email] = {"name": name, "commits_count": 1}

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

        response = await try_get_response(client, BRANCHES_QUERY,
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


async def get_commits_for_branch(repo_name, branch_name, processed_commits, authors_by_emails, lock):
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
                if commit["oid"] in processed_commits:
                    is_over = True
                    break
                if commit["message"].startswith("Merge pull request #"):
                    continue
                processed_commits.add(commit["oid"])
                all_commits.append(commit)

            if not commits["pageInfo"]["hasNextPage"] or is_over:
                break

            cursor = commits["pageInfo"]["endCursor"]

        await client.close_async()
        await update_authors(authors_by_emails, all_commits, lock)



async def get_authors_by_emails():
    authors_by_emails = {}
    cursor = None
    repo_count = 1
    client = get_client()
    authors_update_lock = asyncio.Lock()

    while True:
        tasks = []
        repos_data = await try_get_response(client, REPOS_QUERY, {"cursor": cursor, "org": ORGANIZATION})

        if repos_data is None:
            break

        for repo in repos_data["organization"]["repositories"]["nodes"]:
            print(f'Обрабатывается репозиторий: {repo_count} - {repo["name"]}')
            repo_count += 1
            processed_commits = set()
            repo_name = repo["name"]
            for branch_name in await get_branch_names(repo_name):
                task = asyncio.create_task(
                    get_commits_for_branch(repo_name, branch_name, processed_commits, authors_by_emails,
                                           authors_update_lock))
                tasks.append(task)

        await asyncio.gather(*tasks)

        if not repos_data["organization"]["repositories"]["pageInfo"]["hasNextPage"]:
            break

        cursor = repos_data["organization"]["repositories"]["pageInfo"]["endCursor"]

    await client.close_async()
    return authors_by_emails


def get_client():
    return Client(transport=AIOHTTPTransport(url=GRAPHQL_URL, headers={"Authorization": f"Bearer {TOKEN}"}))

async def main():
    authors_by_emails = await get_authors_by_emails()
    top_100_commiters = sorted(authors_by_emails.items(), key=lambda x: x[1]["commits_count"], reverse=True)[:100]
    process_stat(top_100_commiters)


if __name__ == '__main__':
    asyncio.run(main())
