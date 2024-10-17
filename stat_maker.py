import asyncio

from gql import Client
from gql.transport.aiohttp import AIOHTTPTransport

from visualizer import draw_diagram
from requests import REPOS_QUERY, BRANCHES_QUERY, COMMITS_QUERY, ORGANIZATION

TOKEN = "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"
GRAPHQL_URL = "https://api.github.com/graphql"
SEM = asyncio.Semaphore(5)


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


async def try_get_response(query, variables):
    try:
        return await get_response(query, variables)
    except Exception as e:
        print('Ошибочка!', e)

async def get_response(query, variables):
    client = get_client()
    response = await client.execute_async(query, variable_values=variables)
    await client.close_async()
    return response

async def get_branch_names(repo_name):
    branches_names = []
    cursor = None

    while True:

        response = await try_get_response(BRANCHES_QUERY, {"repo": repo_name, "cursor": cursor, "owner": ORGANIZATION})

        branches = response["repository"]["refs"]
        branches_names.extend(branch["name"] for branch in branches["nodes"])

        if not branches["pageInfo"]["hasNextPage"]:
            break

        cursor = branches["pageInfo"]["endCursor"]

    return branches_names

async def get_commits_for_branch(repo_name, branch_name, processed_commits, authors_by_emails, lock):
    all_commits = []
    is_over = False
    cursor = None

    while True:
        response = await try_get_response(COMMITS_QUERY,
                                         {"repo": repo_name,
                                                   "branch": "refs/heads/" + branch_name,
                                                   "cursor": cursor,
                                                   "owner": ORGANIZATION})
        commit_history = response["repository"]["ref"]["target"]["history"]

        for commit in commit_history["nodes"]:
            if commit["oid"] in processed_commits:
                is_over = True
                break
            if commit["message"].startswith("Merge pull request #"):
                continue
            processed_commits.add(commit["oid"])
            all_commits.append(commit)

        if not commit_history["pageInfo"]["hasNextPage"] or is_over:
            break

        cursor = commit_history["pageInfo"]["endCursor"]

    await update_authors(authors_by_emails, all_commits, lock)


async def process_repo(repo_name, authors_by_emails):
    async with SEM:
        processed_commits = set()
        tasks = []
        lock = asyncio.Lock()
        for branch_name in await get_branch_names(repo_name):
            task = asyncio.create_task(get_commits_for_branch(repo_name, branch_name, processed_commits, authors_by_emails, lock))
            tasks.append(task)
        await asyncio.gather(*tasks)


async def get_commiters():
    authors_by_emails = {}
    variables_for_repos_query = {"org": ORGANIZATION}

    repo_count = 1
    has_next_page = True


    while has_next_page:

        repos_data = await try_get_response(REPOS_QUERY, variables_for_repos_query)

        tasks = []

        for repo in repos_data["organization"]["repositories"]["nodes"]:
            print(f'Обрабатывается репозиторий: {repo_count} - {repo["name"]}')
            repo_count += 1
            tasks.append(asyncio.create_task(process_repo(repo["name"], authors_by_emails)))

        has_next_page = repos_data["organization"]["repositories"]["pageInfo"]["hasNextPage"]
        variables_for_repos_query["cursor"] = repos_data["organization"]["repositories"]["pageInfo"]["endCursor"]

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

