"""Counting deliveries of one message: the cap on a job that kills its worker every time (analyzer#3)."""

from evilflowers_worker.deliveries import TTL_SECONDS, DeliveryCounter


class FakeRedis:
    def __init__(self, fail: bool = False):
        self.fail = fail
        self.values: dict[str, int] = {}
        self.ttl: dict[str, int] = {}

    def _check(self):
        if self.fail:
            raise ConnectionError("redis down")

    def set(self, key, value, ex):
        self._check()
        self.values[key], self.ttl[key] = int(value), ex

    def decr(self, key):
        self._check()
        self.values[key] = self.values.get(key, 0) - 1
        return self.values[key]

    def delete(self, key):
        self.values.pop(key, None)

    def pipeline(self):
        return FakePipeline(self)


class FakePipeline:
    def __init__(self, redis: FakeRedis):
        self.redis = redis
        self.ops: list = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def incr(self, key):
        self.ops.append(("incr", key))
        return self

    def expire(self, key, seconds):
        self.ops.append(("expire", key, seconds))
        return self

    def execute(self):
        self.redis._check()
        results = []
        for op in self.ops:
            if op[0] == "incr":
                self.redis.values[op[1]] = self.redis.values.get(op[1], 0) + 1
                results.append(self.redis.values[op[1]])
            else:
                self.redis.ttl[op[1]] = op[2]
                results.append(True)
        return results


def test_a_fresh_delivery_starts_over_and_redeliveries_add_one():
    redis = FakeRedis()
    counter = DeliveryCounter(redis, prefix="analyzer")
    assert counter.record("task-1", redelivered=False) == 1
    assert counter.record("task-1", redelivered=True) == 2
    assert counter.record("task-1", redelivered=True) == 3
    # The catalog re-runs a book under the same task id: a fresh message starts over, never reaching a false cap.
    assert counter.record("task-1", redelivered=False) == 1
    assert redis.ttl[counter.key("task-1")] == TTL_SECONDS


def test_tasks_and_services_are_counted_apart():
    redis = FakeRedis()
    DeliveryCounter(redis, prefix="analyzer").record("task-1", redelivered=False)
    assert DeliveryCounter(redis, prefix="analyzer").record("task-2", redelivered=True) == 1
    assert DeliveryCounter(redis, prefix="image").record("task-1", redelivered=True) == 1


def test_a_deliberate_requeue_gives_its_delivery_back():
    redis = FakeRedis()
    counter = DeliveryCounter(redis, prefix="analyzer")
    counter.record("task-1", redelivered=False)
    counter.forget(["task-1"])
    assert counter.key("task-1") not in redis.values
    # The requeued message comes back flagged redelivered and counts as the first again.
    assert counter.record("task-1", redelivered=True) == 1


def test_an_unreachable_redis_never_stops_a_job():
    assert DeliveryCounter(FakeRedis(fail=True), prefix="a").record("task-1", redelivered=True) == 1
    assert DeliveryCounter(None, prefix="a").record("task-1", redelivered=True) == 1
    DeliveryCounter(FakeRedis(fail=True), prefix="a").forget(["task-1"])
