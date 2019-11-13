from referenceai.pipeline import Pipeline
import pytest

@pytest.fixture
def p():

    def int_provider(i: int) -> int:
        return i

    def string_provider() -> str:
        return test_sentence

    def int_string_consumer(i: int, s: str) -> str:
        h_str : str = ""
        for _ in range(i):
            h_str += s
        return h_str

    def int_string_provider(i: int, message: str) -> (int, str):
        return (i,message)

    def intermediate_random_provider():
        print("doing nothing at all")

    p = Pipeline("test")
    p.push("sample-1", [string_provider, int_provider, int_string_consumer], revise=False)
    p.push("sample-2", [int_string_provider, intermediate_random_provider, int_string_consumer], revise=False)
    return p

test_sentence = "hello world"

def test_basic_pipeline(p):
    i = 4
    assert(p.run("sample-1", i) == test_sentence*i)

def test_pipeline_cache(p):
    i = 2
    assert(p.run("sample-1", i) == test_sentence*i)
    p.expunge()
    i = 3
    assert(p.run("sample-1", i) == test_sentence*i)

def test_expunge_cache(p):
    r = p.run("sample-1", 3)
    assert (r == test_sentence*3)
    p.expunge()
    p.run("sample-1", 3)
    assert (r == test_sentence*3)

def test_complex_return_types(p):
    r = p.run("sample-2", 4, test_sentence)
    assert (r == test_sentence*4)

