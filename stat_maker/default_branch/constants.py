from gql import gql


ORGANIZATION = 'twitter'
TOKEN = "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"
GRAPHQL_URL = "https://api.github.com/graphql"

REPOS_QUERY = gql(
    """
    query($org: String!, $cursor: String) {
      organization(login: $org) {
        repositories(first: 50, after: $cursor) { 
          pageInfo {
            endCursor
            hasNextPage
          }
          nodes {
            name  
          }
        }
      }
    }
    """
)

COMMITS_QUERY = gql(
    """     
    query($repo: String!, $cursor: String, $owner: String!) {
      repository(name: $repo, owner: $owner) {
        defaultBranchRef {
          target {
            ... on Commit {
              history(first: 100, after: $cursor) {  
                pageInfo {
                  endCursor
                  hasNextPage
                }
                nodes {
                  oid
                  message
                  author {
                    email
                    name
                  }
                }
              }
            }
          }
        }
      }
    }
    """
)