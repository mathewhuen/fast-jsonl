import json


empty_zero = []
empty_ten = [{} for _ in range(10)]
various_ten = [{str(j): (j**2) / 2 for j in range(i)} for i in range(10)]


def save_data(path, data, mode="a"):
    assert isinstance(data, list) or isinstance(data, tuple)
    f = open(path, mode)
    for i, datum in enumerate(data):
        line = json.dumps(datum)
        f.write(line)
        if i < len(data) - 1:
            f.write("\n")
