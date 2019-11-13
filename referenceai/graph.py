from loguru import logger
import networkx as nx
import uuid
import hashlib as hl
import inspect
from functools import reduce
import numpy as np


class GraphRing():
    class Meta():
        def __init__(self, nodes = None, edges = None):
            if nodes is not None:
                self.nodes = nodes
            else:
                self.nodes = {}
            
            if edges is not None:
                self.edges = edges
            else:
                self.edges = {}

    def __init__(self):
        self.cycle = nx.DiGraph()
        self.meta = self.Meta()
        self.meta.nodes['root'] = {'outputs' : []}
        self.cycle.add_node('root')
        self.named_rings = {}
        

    @logger.catch
    def push(self, name, fns):
        def last_index(idx, arr):
            return idx == (len(arr) - 1)

        node_c = 'root'

        for i in range(0,len(fns)):

            fn = fns[i]
            inputs = fn.__annotations__.copy()
            outputs = None

            #assert(len(inputs) > 0) # This module will not work without annotations

            if 'return' in inputs:
                outputs = inputs['return']

            if 'return' in inputs:
                del inputs['return']

            node_id = None

            if last_index(i,fns):
                node_id = 'root'            
            else:
                node_id = str(uuid.uuid1())
                self.cycle.add_node(node_id)
                self.meta.nodes[node_id] = {'outputs' : []}

            e = (node_c, node_id)
            self.meta.edges[e] = {'fn' : fn, 'fn_signature' : None}
            self.cycle.add_edges_from([e])

            if node_c == 'root':
                self.named_rings[name] = ('root', node_id)

            node_c = node_id

    def _find_in_providers(self, inputs, providers):
        fn_providers = []
        fn_providers_not_found = []

        for _, input in inputs.items():
            provider = self._provider_by_class(input, providers)
            if provider is not None:
                fn_providers.append(provider)
            else:
                fn_providers_not_found.append(input)
        return fn_providers, fn_providers_not_found
    
    def _provider_by_class(self, superClassType, providers):
        for p in providers:
            if issubclass(type(p), superClassType):
                return p
        return None

    def _signature(self, fn):
        fn_inputs_signature = fn.__annotations__.copy()
        fn_outputs_signature = None
        
        if 'return' in fn_inputs_signature:
            fn_outputs_signature = fn.__annotations__['return']
            del fn_inputs_signature['return']
        
        return fn_inputs_signature, fn_outputs_signature

    def _filter_by_signature(self, outputs,fn_output_signature):
        signatures = fn_output_signature
        filtered = []
        
        if type(fn_output_signature) is not tuple:
            signatures = (signatures,)
        if type(outputs) is not list:
            outputs = [outputs]

        for signature in signatures:
            output = self._provider_by_class(signature, outputs)
            if output is not None:
                filtered.append(output)
        
        return filtered

    def _execute_or_load_from_cache(self, fn, fn_args, edge):

        def wfn():
            return fn(*fn_args)

        _, fn_outputs_signature = self._signature(fn)

        m = hl.sha256()
        m.update(inspect.getsource(fn).encode('utf-8'))
        signature = m.hexdigest()

        prior_signature = self.meta.edges[edge]['fn_signature']
        if prior_signature is not None and prior_signature == signature:
            # load from cache
            _, to_node = edge

            outputs = self._filter_by_signature(
                self.meta.nodes[to_node]['outputs'],
                fn_outputs_signature)

            if outputs is not None:
                return outputs, True
        
        self.meta.edges[edge]['fn_signature'] = signature
        return wfn(), False

    def _execute_edge(self, edge : tuple, providers = []):

        from_node, _ = edge
        fn = self.meta.edges[edge]['fn']
        
        fn_inputs_signature, _ = self._signature(fn)
        fn_args, fn_args_not_found = self._find_in_providers(fn_inputs_signature, providers)
        
        if len(fn_args_not_found) > 0:
            prior_node = next(self.cycle.predecessors(from_node))
            prior_edge = (prior_node, from_node)
            for p in self._execute_edge(prior_edge, providers = providers):
                providers.insert(0, p)
            fn_args, _ = self._find_in_providers(fn_inputs_signature, providers)

        rtn, cached = self._execute_or_load_from_cache(fn, fn_args, edge)
        if type(rtn) is not tuple:
            rtn = (rtn,)

        for p in rtn:
            providers.insert(0, p)
        
        return fn, rtn, cached, providers

    @logger.catch
    def run(self, name : str, *args):
        from_node, to_node = self.named_rings[name]
        e = (from_node, to_node)
        providers = []
        if len(args) > 0:
            providers = [*args]
        
        # add all providers from root node
        for p in self.meta.nodes['root']['outputs']:
            if type(p) not in list(map(lambda x: type(x), providers)):
                providers.append(p)


        # invalidate ring outputs if the inputs are not the same
        if 'inputs' in self.meta.edges[e]:
            valid = True
            for item1, item2 in list(zip(self.meta.edges[e]['inputs'], args)):
                if type(item1) == type(item2) == np.ndarray:
                    valid = valid and (np.array_equal(item1,item2))
                else:
                    valid = valid and (item1 == item2)
            if not valid: self.expunge()
        self.meta.edges[e]['inputs'] = args

        rtn = None

        fns = []
        rtns = []

        while True:
            
            # execute and advance
            fn, rtn, cached, providers = self._execute_edge(e, providers = providers)
            
            fns.append(fn)
            rtns.append(rtn)

            if not cached:

                for r in rtn:
                    idx = None
                    for i, o in enumerate(self.meta.nodes[to_node]['outputs']):
                        if type(r) == type(o):
                            idx = i                            
                            break

                    if idx is None:
                        self.meta.nodes[to_node]['outputs'].append(r)
                    else:
                        self.meta.nodes[to_node]['outputs'][idx] = r

            # terminate
            if to_node == 'root':
                break

            # advance
            from_node = to_node
            to_node = next(self.cycle.successors(to_node)) # Since these are rings there should only be one successor
            e = (from_node, to_node)

        if len(rtn) == 1:
            rtn = rtn[0]

        return rtn, zip(fns, rtns)

    def expunge(self):
        for node_id, _  in self.meta.nodes.items():
            self.meta.nodes[node_id]['outputs'] = []
        for e, _ in self.meta.edges.items():
            self.meta.edges[e]['fn_signature'] = None