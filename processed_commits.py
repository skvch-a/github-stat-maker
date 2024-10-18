from asyncio import Lock

class ProcessedCommits:
    def __init__(self):
        self._hashes = set()
        self._lock = Lock()

    async def add(self, commit_hash):
        async with self._lock:
            self._hashes.add(commit_hash)

    async def contains(self, commit_hash):
        async with self._lock:
            return commit_hash in self._hashes