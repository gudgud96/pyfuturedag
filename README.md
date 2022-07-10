## PyFutureDAG

A simple, lightweight Python DAG implementation using [asyncio](https://docs.python.org/3/library/asyncio.html) futures, in the style of [C++ folly FutureDAG](https://github.com/facebook/folly/blob/main/folly/experimental/FutureDAG.h).

### Installation
```
pip install pyfuturedag
```

### Demo
```python
"""
          1  -  3
      /             \
    0                 4
      \             /
             2
"""
from pyfuturedag import FutureDAG, Node

# example of a function
def print_func(num):
    print("printing", num)

# initialize dag
dag = FutureDAG()

# create nodes - Node(func, args)
node_0 = Node(print_func, "0")
node_0_idx = dag.add(node_0)
node_1 = Node(print_func, "1")
node_1_idx = dag.add(node_1)
node_2 = Node(print_func, "2")
node_2_idx = dag.add(node_2)
node_3 = Node(print_func, "3")
node_3_idx = dag.add(node_3)
node_4 = Node(print_func, "4")
node_4_idx = dag.add(node_4)

# specify dependency
dag.dependency(node_0_idx, node_1_idx)
dag.dependency(node_0_idx, node_2_idx)
dag.dependency(node_1_idx, node_3_idx)
dag.dependency(node_2_idx, node_4_idx)
dag.dependency(node_3_idx, node_4_idx)

# run dag
dag.loop.run_until_complete(dag.go())
```

### Test
Simply run `pytest` to run tests.