import asyncio
import functools
from time import time
import random

async def my_callback(result):
    print("my_callback got:", result)
    return "My return value is ignored"

def set_future(node, *args):
    print("setting future now", node, node.future)
    node.future.set_result(1)


async def coro(number):
    await asyncio.sleep(number)
    return number + 1


async def add_success_callback(fut, callback):
    result = await fut
    await callback(result)
    return result

async def run_dag_task(func, args, futs):
    await asyncio.gather(*futs)
    await func(args)

async def print_func(args):
    secs = random.randint(1,5)
    print("sleeping", secs, args)
    await asyncio.sleep(secs)
    print("printing", args)


async def sink_func(args):
    print("sink node")


async def source_func(args):
    print("source node")


class Node:
    def __init__(self, func, args):
        self.func = func
        self.args = args
        self.future = None     # to be awaited, fulfill when node.func is done
        self.dependencies = []
        self.has_dependents = False
        self.visited = False
        self.idx = -1


class FutureDAG:
    def __init__(self):
        self.nodes = []
        self.loop = asyncio.new_event_loop()

    def add(self, node: Node) -> int:
        node.future = self.loop.create_future()
        self.nodes.append(node)
        node.idx = len(self.nodes) - 1
        return node.idx  # index of the node 

    def remove(self, node_idx: int) -> None:
        if node_idx >= len(self.nodes):
            return
        if self.nodes[node_idx].has_dependents:
            for _, node in enumerate(self.nodes):
                deps = node.dependencies
                deps.remove(node_idx)   # delete node_idx from dependencies
                for i in range(len(deps)):
                    if deps[i] > node_idx:
                        deps[i] -= 1
        
        del self.nodes[node_idx]

    def dependency(self, from_idx, to_idx):
        # from_idx -> to_idx, from_idx is dependency, to_idx is dependent
        self.nodes[to_idx].dependencies.append(from_idx)
        self.nodes[from_idx].has_dependents = True

    def has_cycle(self):
        dependencies = []
        for node in self.nodes:
            dependencies.append(node.dependencies.copy())
        dependents = [0 for _ in range(len(self.nodes))]
        for dep in dependencies:
            for node_idx in dep:
                dependents[node_idx] += 1
        
        # topological sort, repeatedly remove sink
        cur_sinks = []
        for node_idx in range(len(dependents)):
            if dependents[node_idx] == 0:
                cur_sinks.append(node_idx)
        
        while len(cur_sinks) > 0:
            # print('sinks', cur_sinks)
            # print('dependencies', dependencies)
            # print('dependents', dependents)

            # remove sink
            cur_sink_idx = cur_sinks.pop()
            # remove dependencies of cur_sink
            while len(dependencies[cur_sink_idx]) > 0:
                dep = dependencies[cur_sink_idx].pop()
                dependents[dep] -= 1
                if dependents[dep] == 0:
                    cur_sinks.append(dep)
        
        has_cycle = False
        for dep in dependencies:
            if len(dep) > 0:
                has_cycle = True
        
        return has_cycle

    async def go(self):
        if self.has_cycle():
            raise Exception("DAG has cycle, exitting...")
        
        for i in range(len(self.nodes)):
            print(self.nodes[i], 'inside', i, dag.nodes[i].dependencies)
        
        # collect roots and leafs
        root_nodes_idx = []
        leaf_nodes_idx = []
        for i in range(len(self.nodes)):
            cur_node = self.nodes[i]
            if len(cur_node.dependencies) == 0:
                root_nodes_idx.append(i)
            if not cur_node.has_dependents:
                leaf_nodes_idx.append(i)
        
        # create an empty sink node, link leaf nodes to sink
        sink_node = Node(sink_func, None)
        sink_node_idx = self.add(sink_node)
        for node_idx in leaf_nodes_idx:
            self.dependency(node_idx, sink_node_idx)
        print(sink_node, 'inside sink node', sink_node.dependencies)
        
        # create a empty source node, link source node to roots
        source_node = Node(source_func, None) 
        source_node_idx = self.add(source_node)
        for node_idx in root_nodes_idx:
            self.dependency(source_node_idx, node_idx)
        print(source_node, 'inside source node', source_node.dependencies)

        for idx, node in enumerate(self.nodes):
            cur_dependencies_tasks = []

            # get all dependencies
            for dep_idx in node.dependencies:
                print('idx', idx, 'dep_idx', dep_idx)
                cur_dependencies_tasks.append(self.nodes[dep_idx].future)
            
            # then run current node's func, which should be a task
            task = self.loop.create_task(run_dag_task(node.func, node.args, cur_dependencies_tasks))
            print('task', task)
            task.add_done_callback(functools.partial(set_future, node))  # TODO: callback should fulfill promise

            # TODO: should do error handling for each node

        await self.nodes[sink_node_idx].future
        return self.nodes[sink_node_idx].future.result() == 1


if __name__ == "__main__":
    """
    run this DAG
          1  -  3
      /             \
    0         \        5
      \             /
          2  -  4
    """
    dag = FutureDAG()
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
    node_5 = Node(print_func, "5")
    node_5_idx = dag.add(node_5)
    node_6 = Node(print_func, "6")
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





            
            