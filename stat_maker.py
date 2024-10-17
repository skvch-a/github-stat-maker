import asyncio

from gql import Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.exceptions import TransportQueryError

from visualizer import draw_diagram
from requests import REPOS_QUERY, BRANCHES_QUERY, COMMITS_QUERY, ORGANIZATION

sem = asyncio.Semaphore(8)
TOKEN = "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"
GRAPHQL_URL = "https://api.github.com/graphql"
REPOS_OVER = {}

def get_top_100(authors_by_emails):
    return sorted(authors_by_emails.items(), key=lambda x: x[1]["commits_count"], reverse=True)[:100]

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
        tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
        await asyncio.gather(*tasks, return_exceptions=True)
        print('Rate limit exceeded, try again later or change TOKEN')
        exit()
    except Exception:
        print(f'Непонятная ошибка на запросе {variables}')



async def get_branch_names(repo_name):
    branches_names = []
    cursor = None
    client = get_client()
    while True:

        response = await try_get_response(client, BRANCHES_QUERY, {"repo": repo_name, "cursor": cursor, "owner": ORGANIZATION})
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
    if repo_name in REPOS_OVER:
        return []
    async with sem:
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
            commit_history = response["repository"]["ref"]["target"]["history"]
            k = 0
            for commit in commit_history["nodes"]:
                if commit["oid"] in processed_commits:
                    if k == 0:
                        REPOS_OVER[repo_name] = True
                    is_over = True
                    break
                if commit["message"].startswith("Merge pull request #"):
                    continue
                processed_commits.add(commit["oid"])
                all_commits.append(commit)

            if not commit_history["pageInfo"]["hasNextPage"] or is_over:
                break

            cursor = commit_history["pageInfo"]["endCursor"]

        await client.close_async()
        await update_authors(authors_by_emails, all_commits, lock)



async def get_commiters():
    authors_by_emails = {}
    variables_for_repos_query = {"org": ORGANIZATION}
    client = get_client()
    repo_count = 1
    has_next_page = True
    lock = asyncio.Lock()
    while has_next_page:
        tasks = []
        repos_data = await try_get_response(client, REPOS_QUERY, variables_for_repos_query)
        if repos_data is None:
            break
        for repo in repos_data["organization"]["repositories"]["nodes"]:
            print(f'Обрабатывается репозиторий: {repo_count} - {repo["name"]}')
            repo_count += 1
            processed_commits = set()
            repo_name = repo["name"]
            for branch_name in await get_branch_names(repo_name):
                task = asyncio.create_task(
                    get_commits_for_branch(repo_name, branch_name, processed_commits, authors_by_emails, lock))
                tasks.append(task)

        await asyncio.gather(*tasks)

        has_next_page = repos_data["organization"]["repositories"]["pageInfo"]["hasNextPage"]
        variables_for_repos_query["cursor"] = repos_data["organization"]["repositories"]["pageInfo"]["endCursor"]

    await client.close_async()
    return authors_by_emails


def get_client():
    authorization_header = {"Authorization": f"Bearer {TOKEN}"}
    transport = AIOHTTPTransport(url=GRAPHQL_URL, headers=authorization_header)
    return Client(transport=transport)


async def main():
    commiters = await get_commiters()
    top_100_commiters = get_top_100(commiters)
    draw_diagram(top_100_commiters)


if __name__ == '__main__':
    asyncio.run(main())

