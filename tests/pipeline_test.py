from referenceai.pipeline import Pipeline


def int_provider(i: int) -> int:
    return i

def string_provider() -> str:
    return "hello world."

def int_string_consumer(i: int, s: str) -> str:
    h_str : str = ""
    for _ in range(i):
        h_str += s
    return h_str

def test_basic_pipeline():
    p = Pipeline("test")

    p.push(int_provider)
    p.push(string_provider)
    p.push(int_string_consumer)

    i = 3

    assert(p.run(i) == ("hello world.")*i)
