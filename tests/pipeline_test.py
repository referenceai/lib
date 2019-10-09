from referenceai.pipeline import Pipeline

test_sentence = "hello world"

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
p.push(int_provider)
p.push(string_provider)
p.push(int_string_consumer)


def test_basic_pipeline():
    i = 4
    assert(p.run(i) == test_sentence*i)

def test_pipeline_cache():
    i = 2
    assert(p.run(i) == test_sentence*i)
    i = 3
    assert(p.run(i) == test_sentence*i)

def test_expunge_cache():
    r = p.run(3)
    assert (r == test_sentence*3)
    p.expunge_cache()
    p.run(3)
    assert (r == test_sentence*3)


p2 = Pipeline("something")
p2.push(int_string_provider)
p2.push(intermediate_random_provider)
p2.push(int_string_consumer)

def test_complex_return_types():
    r = p2.run(4,test_sentence)
    assert (r == test_sentence*4)

