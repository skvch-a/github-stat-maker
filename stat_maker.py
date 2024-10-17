import asyncio

from gql import Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.exceptions import TransportQueryError

from visualizer import draw_diagram
from requests import REPOS_QUERY, BRANCHES_QUERY, COMMITS_QUERY, ORGANIZATION

TOKEN = "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"
GRAPHQL_URL = "https://api.github.com/graphql"

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
        print('Rate limit exceeded, try again later or change TOKEN')
        exit()

async def get_branch_names(repo_name):
    branches_names = []
    cursor = None
    client = get_client()
    while True:

        response = await try_get_response(client, BRANCHES_QUERY, {"repo": repo_name, "cursor": cursor, "owner": ORGANIZATION})

        branches = response["repository"]["refs"]
        branches_names.extend(branch["name"] for branch in branches["nodes"])

        if not branches["pageInfo"]["hasNextPage"] or True:
            break

        cursor = branches["pageInfo"]["endCursor"]

    await client.close_async()
    return branches_names

async def get_commits_for_branch(repo_name, branch_name, processed_commits, authors_by_emails, lock):
    print(f'Ветка {branch_name} репозитория {repo_name}')
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
            print('ХУЕТА!!!!!!!!')
            break
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
    await client.close_async()
    await update_authors(authors_by_emails, all_commits, lock)


async def process_repo(repo_name, authors_by_emails):

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
    client = get_client()
    repo_count = 1
    has_next_page = True


    while has_next_page:
        tasks = []
        repos_data = await try_get_response(client, REPOS_QUERY, variables_for_repos_query)

        for repo in repos_data["organization"]["repositories"]["nodes"]:
            print(f'Обрабатывается репозиторий: {repo_count} - {repo["name"]}')
            repo_count += 1
            tasks.append(asyncio.create_task(process_repo(repo["name"], authors_by_emails)))
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

