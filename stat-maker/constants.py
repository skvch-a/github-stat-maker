from gql import gql


ORGANIZATION = 'godaddy'
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

BRANCHES_QUERY = gql(
    """
    query($repo: String!, $cursor: String, $owner: String!) {
      repository(name: $repo, owner: $owner) {
        refs(first: 100, after: $cursor, refPrefix: "refs/heads/") {  
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
    query($repo: String!, $branch: String!, $cursor: String, $owner: String!) {
      repository(name: $repo, owner: $owner) {
        ref(qualifiedName: $branch) {
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