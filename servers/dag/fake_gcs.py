"""In-memory stand-in for google-cloud-storage, for tests.

Implements just enough of the Client / Bucket / Blob surface the ledger code
touches — including real generation numbering and if_generation_match
compare-and-swap semantics — so tests can exercise the genuine
optimistic-concurrency paths (hitl decision consume, blocker triage,
review-decision writes) without a network or a filesystem backend.

The compare-and-swap contract mirrors GCS exactly:
  * if_generation_match=0   -> write only if the object does not exist
  * if_generation_match=N   -> write only if the current generation is N
  * if_generation_match=None -> unconditional write
Generations are unique, monotonically increasing integers (>0) assigned by the
client on each successful write, so a freshly-created object never collides with
the "must be absent" sentinel 0. Mismatches raise the real
google.api_core.exceptions.PreconditionFailed, and reads of a missing object
raise the real exceptions.NotFound, so callers' except clauses match unchanged.
"""

from google.api_core import exceptions


class FakeBlob:
    """A handle to one object path within a FakeBucket.

    Like a real Blob, the handle is cheap and stateless about content: the data
    lives in the bucket's store, and `generation` is only populated once the
    handle has read (reload/download) or written (upload) the object.
    """

    def __init__(self, bucket, name):
        self._bucket = bucket
        self.name = name
        self.generation = None

    def reload(self, *args, **kwargs):
        rec = self._bucket._store.get(self.name)
        if rec is None:
            raise exceptions.NotFound(
                f"No such object: {self._bucket.name}/{self.name}"
            )
        self.generation = rec["generation"]

    def exists(self, *args, **kwargs):
        return self.name in self._bucket._store

    def download_as_text(self, *args, **kwargs):
        rec = self._bucket._store.get(self.name)
        if rec is None:
            raise exceptions.NotFound(
                f"No such object: {self._bucket.name}/{self.name}"
            )
        self.generation = rec["generation"]
        return rec["data"]

    def download_as_bytes(self, *args, **kwargs):
        return self.download_as_text().encode("utf-8")

    def upload_from_string(self, data, content_type=None, if_generation_match=None,
                           **kwargs):
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        rec = self._bucket._store.get(self.name)
        current_gen = rec["generation"] if rec else 0
        if if_generation_match is not None and if_generation_match != current_gen:
            raise exceptions.PreconditionFailed(
                f"Generation mismatch on {self._bucket.name}/{self.name}: "
                f"expected {if_generation_match}, have {current_gen}"
            )
        new_gen = self._bucket._client._next_generation()
        self._bucket._store[self.name] = {
            "data": data,
            "content_type": content_type,
            "generation": new_gen,
        }
        self.generation = new_gen

    def delete(self, *args, **kwargs):
        if self._bucket._store.pop(self.name, None) is None:
            raise exceptions.NotFound(
                f"No such object: {self._bucket.name}/{self.name}"
            )


class _FakeIamConfiguration:
    def __init__(self):
        self.uniform_bucket_level_access_enabled = False
        self.public_access_prevention = None


class FakeBucket:
    def __init__(self, client, name):
        self._client = client
        self.name = name
        self._store = {}
        self.iam_configuration = _FakeIamConfiguration()

    def blob(self, name):
        return FakeBlob(self, name)

    def list_blobs(self, prefix=None, **kwargs):
        return self._client.list_blobs(self, prefix=prefix)

    def create(self, project=None, **kwargs):
        # Real GCS would 409 on an existing bucket; the provisioning tests that
        # care about that path use a MagicMock, so this is a no-op here.
        return None

    def reload(self, *args, **kwargs):
        return None

    def patch(self, *args, **kwargs):
        return None


class FakeStorageClient:
    """Drop-in for google.cloud.storage.Client backed by process memory.

    bucket(name) returns the same FakeBucket instance for a given name for the
    client's lifetime, so writes through one handle are visible through another
    — matching how the real client shares one backing store per bucket.
    """

    def __init__(self, project=None, **kwargs):
        self.project = project
        self._buckets = {}
        self._gen_counter = 0

    def _next_generation(self):
        self._gen_counter += 1
        return self._gen_counter

    def bucket(self, name):
        bucket = self._buckets.get(name)
        if bucket is None:
            bucket = FakeBucket(self, name)
            self._buckets[name] = bucket
        return bucket

    def create_bucket(self, name, **kwargs):
        return self.bucket(name if isinstance(name, str) else name.name)

    def list_blobs(self, bucket_or_name, prefix=None, **kwargs):
        name = bucket_or_name.name if isinstance(bucket_or_name, FakeBucket) else bucket_or_name
        bucket = self.bucket(name)
        blobs = []
        for path in sorted(bucket._store):
            if prefix is None or path.startswith(prefix):
                blob = FakeBlob(bucket, path)
                blob.generation = bucket._store[path]["generation"]
                blobs.append(blob)
        return blobs
