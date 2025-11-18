import random
import string
import timeit
from collections import Counter

from list_named_entities import Processor


def gen_entities(n: int, uniq_ratio: float = 0.6) -> list[tuple[str, str]]:
    # n total pairs; ~uniq_ratio*n unique values, repeated randomly
    uniq_count = max(1, int(n * uniq_ratio))
    labels = ['PERSON', 'GPE', 'LOC', 'ORG', 'PRODUCT']
    uniques = []
    for i in range(uniq_count):
        s = ''.join(random.choice(string.ascii_letters + ' ') for _ in range(random.randint(5, 15))).strip()
        label = random.choice(labels)
        uniques.append((s, label))
    data = [random.choice(uniques) for _ in range(n)]
    return data


def current_impl(cleaned: list[tuple[str, str]]) -> list[tuple[tuple[str, str], int]]:
    p = Processor()
    p.cleaned_entities = cleaned
    p.make_uniques()
    return p.sorted_unique_entries


def one_liner_impl(cleaned: list[tuple[str, str]]) -> list[tuple[tuple[str, str], int]]:
    named_entity_counts: Counter = Counter(cleaned)
    return sorted(named_entity_counts.items(), key=lambda kv: (kv[0][0].lower(), kv[0][1]))


for n in (1_000, 5_000, 20_000):
    cleaned = gen_entities(n, uniq_ratio=0.7)

    # Warm up
    current_impl(cleaned)
    one_liner_impl(cleaned)

    t_current = timeit.timeit(lambda: current_impl(cleaned), number=10)
    t_one = timeit.timeit(lambda: one_liner_impl(cleaned), number=10)
    print(f'n={n:>6} current_impl: {t_current:.4f}s  one_liner: {t_one:.4f}s  ratio={t_current / t_one:.2f}x')
