from pubtools._pulp.tasks.push.command import batched_items


def test_batched_items_empty():
    """Batching an empty list yields an empty list."""
    assert [] == list(batched_items([]))


def test_batched_items_batches():
    """Batched items splits into batches of the specified size."""
    assert [
        [0, 1, 2],
        [3, 4, 5],
        [6, 7, 8],
        [9],
    ] == list(batched_items(range(10), batchsize=3))
