"""
Core functions for pyfuturedag
"""
import asyncio
import logging
import enum


class TaskState(enum.Enum):
   SUCCESS = 1
   ERROR = -1
   DEP_FAILED = -80101
   TIMEOUT = -80103


async def run_dag_task(node, futs):
    # await dependency tasks for current node
    fut_results = await asyncio.gather(*futs)
    deps_success = True
    for state in fut_results:
        if state != TaskState.SUCCESS:
            deps_success = False
            break
    if not deps_success:
        logging.debug("not running node {} due to one of its deps failed".format(node.idx))
        node.future.set_result(TaskState.DEP_FAILED)
        return

    # run func for current node
    try:
        if node.timeout_secs > 0:
            await asyncio.wait_for(
                node.func(node.args),
                timeout = node.timeout_secs
            )
        else:
            await node.func(node.args)
        node.future.set_result(TaskState.SUCCESS)
    except asyncio.TimeoutError:
        logging.error("node {} exceed timeout of {} secs".format(node.idx, node.timeout_secs))
        node.future.set_result(TaskState.TIMEOUT)
    except Exception as e:
        logging.error("error thrown when running node: {}, {}".format())
        node.future.set_result(TaskState.ERROR)


async def sink_func(args):
    logging.debug("Running sink node")


async def source_func(args):
    logging.debug("Running source node")


class Node:
    def __init__(self, func, args, timeout_secs=-1):
        self.func = func
        self.args = args
        self.future = None      # to be awaited, fulfill when node.func is done
        self.dependencies = []
        self.has_dependents = False
        self.visited = False
        self.idx = -1
        self.timeout_secs = timeout_secs  # -1 does not timeout


class FutureDAG:
    def __init__(self):
        self.nodes = []
        self.loop = asyncio.new_event_loop()
        self.final_state = 0

    def add(self, node: Node) -> int:
        node.future = self.loop.create_future()
        self.nodes.append(node)
        node.idx = len(self.nodes) - 1
        return node.idx

    def remove(self, node_idx: int) -> None:
        if node_idx >= len(self.nodes):
            return
        if self.nodes[node_idx].has_dependents:
            # delete node_idx from dependencies
            for _, node in enumerate(self.nodes):
                deps = node.dependencies
                deps.remove(node_idx)   
                for i in range(len(deps)):
                    if deps[i] > node_idx:
                        deps[i] -= 1
        
        del self.nodes[node_idx]

    def dependency(self, from_idx, to_idx) -> None:
        # from_idx -> to_idx, from_idx is dependency, to_idx is dependent
        self.nodes[to_idx].dependencies.append(from_idx)
        self.nodes[from_idx].has_dependents = True

    def has_cycle(self) -> bool:
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

    async def go(self) -> bool:
        if self.has_cycle():
            raise Exception("DAG has cycle, exitting...")
        
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
        
        # create a empty source node, link source node to roots
        source_node = Node(source_func, None) 
        source_node_idx = self.add(source_node)
        for node_idx in root_nodes_idx:
            self.dependency(source_node_idx, node_idx)

        for _, node in enumerate(self.nodes):
            cur_dependencies_tasks = []

            # get all dependencies
            for dep_idx in node.dependencies:
                cur_dependencies_tasks.append(self.nodes[dep_idx].future)
            
            # then run current node's func, which should be a task
            self.loop.create_task(
                run_dag_task(
                    node, 
                    cur_dependencies_tasks
                )
            )

        await self.nodes[sink_node_idx].future

        # dag's state is equivalent to sink's task state
        self.final_state = self.nodes[sink_node_idx].future.result()
        if self.final_state != TaskState.SUCCESS:
            logging.error("DAG run is not complete!")
        return self.final_state == TaskState.SUCCESS