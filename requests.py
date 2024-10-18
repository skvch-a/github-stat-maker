from gql import gql

ORGANIZATION = "godaddy"

REPOS_QUERY = gql(
    """
    query($org: String!, $cursor: String) {
      organization(login: $org) {
        repositories(first: 10, after: $cursor) { 
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