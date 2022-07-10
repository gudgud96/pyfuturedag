from pyfuturedag import FutureDAG, Node, TaskState
import asyncio


async def print_func_append(ctx):
    print("printing", ctx.idx)
    ctx.lst.append(ctx.idx)
    await asyncio.sleep(1)


class TestContext:
    def __init__(self, idx, lst):
        self.idx = idx
        self.lst = lst


def test_case_1():
    """
    Run this DAG
          1  -  3
      /             \
    0         \        5
      \             /
          2  -  4
      \ 
          6  

    Expect node_order_lst to be in order.  
    """
    dag = FutureDAG()
    node_order_lst = []

    node_0 = Node(print_func_append, TestContext("0", node_order_lst))
    node_0_idx = dag.add(node_0)
    node_1 = Node(print_func_append, TestContext("1", node_order_lst))
    node_1_idx = dag.add(node_1)
    node_2 = Node(print_func_append, TestContext("2", node_order_lst))
    node_2_idx = dag.add(node_2)
    node_3 = Node(print_func_append, TestContext("3", node_order_lst))
    node_3_idx = dag.add(node_3)
    node_4 = Node(print_func_append, TestContext("4", node_order_lst))
    node_4_idx = dag.add(node_4)
    node_5 = Node(print_func_append, TestContext("5", node_order_lst))
    node_5_idx = dag.add(node_5)
    node_6 = Node(print_func_append, TestContext("6", node_order_lst))
    node_6_idx = dag.add(node_6)

    dag.dependency(node_0_idx, node_1_idx)
    dag.dependency(node_0_idx, node_2_idx)
    dag.dependency(node_1_idx, node_3_idx)
    dag.dependency(node_2_idx, node_4_idx)
    dag.dependency(node_1_idx, node_4_idx)
    dag.dependency(node_3_idx, node_5_idx)
    dag.dependency(node_4_idx, node_5_idx)
    dag.dependency(node_0_idx, node_6_idx)

    dag.loop.run_until_complete(dag.go())

    # assert dependencies
    assert node_order_lst.index("0") <  node_order_lst.index("1")
    assert node_order_lst.index("0") <  node_order_lst.index("2")
    assert node_order_lst.index("1") <  node_order_lst.index("3")
    assert node_order_lst.index("2") <  node_order_lst.index("4")
    assert node_order_lst.index("1") <  node_order_lst.index("4")
    assert node_order_lst.index("3") <  node_order_lst.index("5")
    assert node_order_lst.index("4") <  node_order_lst.index("5")
    assert node_order_lst.index("0") <  node_order_lst.index("6")

    # assert dag state
    assert dag.final_state == TaskState.SUCCESS


def test_case_2():
    """
    run this DAG
          1  -  3
      /             \
    0                 5
      \             /
          2  -  4
      \ 
          6  -  7
    
    but 1 timeout.
    
    Expect 0, 1, 2, 4, 6, 7 to run
    """
    dag = FutureDAG()
    node_order_lst = []

    node_0 = Node(print_func_append, TestContext("0", node_order_lst))
    node_0_idx = dag.add(node_0)

    # add a short timeout to fail this node
    node_1 = Node(print_func_append, TestContext("1", node_order_lst), 0.1)
    node_1_idx = dag.add(node_1)
    
    node_2 = Node(print_func_append, TestContext("2", node_order_lst))
    node_2_idx = dag.add(node_2)
    node_3 = Node(print_func_append, TestContext("3", node_order_lst))
    node_3_idx = dag.add(node_3)
    node_4 = Node(print_func_append, TestContext("4", node_order_lst))
    node_4_idx = dag.add(node_4)
    node_5 = Node(print_func_append, TestContext("5", node_order_lst))
    node_5_idx = dag.add(node_5)
    node_6 = Node(print_func_append, TestContext("6", node_order_lst))
    node_6_idx = dag.add(node_6)
    node_7 = Node(print_func_append, TestContext("7", node_order_lst))
    node_7_idx = dag.add(node_7)

    dag.dependency(node_0_idx, node_1_idx)
    dag.dependency(node_0_idx, node_2_idx)
    dag.dependency(node_1_idx, node_3_idx)
    dag.dependency(node_2_idx, node_4_idx)
    dag.dependency(node_3_idx, node_5_idx)
    dag.dependency(node_4_idx, node_5_idx)
    dag.dependency(node_0_idx, node_6_idx)
    dag.dependency(node_6_idx, node_7_idx)

    dag.loop.run_until_complete(dag.go())

    # assert successful nodes
    for elem in ["0", "1", "2", "4", "6", "7"]:
        assert elem in node_order_lst
    for elem in ["3", "5"]:
        assert elem not in node_order_lst

    # assert dependencies
    assert node_order_lst.index("0") <  node_order_lst.index("1")
    assert node_order_lst.index("0") <  node_order_lst.index("2")
    assert node_order_lst.index("2") <  node_order_lst.index("4")
    assert node_order_lst.index("0") <  node_order_lst.index("6")
    assert node_order_lst.index("6") <  node_order_lst.index("7")

    # assert dag state
    assert dag.final_state == TaskState.DEP_FAILED            