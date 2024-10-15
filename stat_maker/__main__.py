import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

from gql import Client
from gql.transport.requests import RequestsHTTPTransport

from .commiters import Commiters
from .requests import REPOS_QUERY, BRANCHES_QUERY, COMMITS_QUERY, GRAPHQL_URL


def get_commiters():
    commiters = Commiters()
    variables_for_repos_query = {"org": ORGANIZATION}
    client = get_client()
    repo_count = 1
    has_next_page = True

    while has_next_page:
        repos_data = client.execute(REPOS_QUERY, variable_values=variables_for_repos_query)

        for repo in repos_data["organization"]["repositories"]["nodes"]:
            print(f'Обрабатывается репозиторий: {repo_count} - {repo["name"]}')
            repo_count += 1
            processed_commits = set()
            branch_count = 1
            for branch_name in get_branch_names(repo["name"], client):
                print(f'\rОбрабатывается ветка: {branch_count} - {branch_name}                     ', end='')
                branch_count += 1
                commit_history = get_commits_for_branch(repo["name"], branch_name, client, processed_commits)
                commiters.update(commit_history)
            print()
        has_next_page = repos_data["organization"]["repositories"]["pageInfo"]["hasNextPage"]
        variables_for_repos_query["cursor"] = repos_data["organization"]["repositories"]["pageInfo"]["endCursor"]

    return commiters


def get_branch_names(repo_name, client):
    branches_names = []
    cursor = None

    while True:
        response = client.execute(BRANCHES_QUERY,
                                  variable_values={"repo": repo_name, "cursor": cursor, "owner": ORGANIZATION})
        branches = response["repository"]["refs"]
        branches_names.extend(branch["name"] for branch in branches["nodes"])

        if not branches["pageInfo"]["hasNextPage"]:
            break

        cursor = branches["pageInfo"]["endCursor"]

    return branches_names


def get_commits_for_branch(repo_name, branch_name, client, processed_commits):
    all_commits = []
    is_over = False
    cursor = None

    while True:
        response = client.execute(COMMITS_QUERY,
                                  variable_values={"repo": repo_name, "branch": "refs/heads/" + branch_name,
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

    return all_commits


def process_stat(stat):
    authors = []
    commits_counts = []
    for email, author_info in stat:
        authors.append(f'{author_info["name"]} ({email})')
        commits_counts.append(author_info["commits_count"])
    return authors, commits_counts


def draw_diagram(stat):
    authors, commits_counts = process_stat(stat)
    plt.figure(figsize=(8, 16))
    sns.barplot(x=commits_counts, y=authors, hue=commits_counts, palette="viridis", legend=False)
    plt.xlabel('Число коммитов')
    plt.title(f'Топ 100 авторов коммитов {ORGANIZATION}')
    plt.xticks(fontsize=9)
    plt.yticks(fontsize=9)
    plt.yticks(np.arange(len(authors)), labels=authors, rotation=0)
    plt.tight_layout()
    plt.savefig(f'top_100_{ORGANIZATION}_commiters.jpg', dpi=300, bbox_inches='tight')
    plt.show()


def get_client():
    authorization_header = {"Authorization": f"Bearer {TOKEN}"}
    transport = RequestsHTTPTransport(url=GRAPHQL_URL, headers=authorization_header, use_json=True)
    return Client(transport=transport, fetch_schema_from_transport=True)


def main():
    commiters = get_commiters()
    top_100_commiters = commiters.get_top_100()
    draw_diagram(top_100_commiters)


if __name__ == '__main__':
    TOKEN = input('Введите свой GITHUB TOKEN (для увеличения rate limit): ')
    ORGANIZATION = input('Введите имя организации: ')
    main()
