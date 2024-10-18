from asyncio import Lock

class CommitersData:
    def __init__(self):
        self._authors_by_emails = {}
        self._lock = Lock()

    def get_top_100(self):
        return sorted(self._authors_by_emails.items(), key=lambda x: x[1]["commits_count"], reverse=True)[:100]

    async def update(self, commits):
        for commit in commits:
            name = commit["author"]["name"]
            email = commit["author"]["email"]

            async with self._lock:
                if email in self._authors_by_emails:
                    self._authors_by_emails[email]["commits_count"] += 1
                else:
                    self._authors_by_emails[email] = {"name": name, "commits_count": 1}