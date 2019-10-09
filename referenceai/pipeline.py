import pickle
import os
from os import path, makedirs

class Pipeline():

    provided_types = []
    fns = []
    providers = {}

    base_path = None

    def __init__(self, base_path = None):
        if base_path is None:
            self.base_path = path.join(os.getcwd(), ".rai", "cache")
        if not path.exists(self.base_path):
            makedirs(self.base_path)

    def push(self, fn):
        # transform must be a function
        assert(callable(fn))
        # now we are going to verify the inputs
        # and make sure that something in the pipeline
        # is ready to supply it
        
        inputs = fn.__annotations__.copy()
        outputs = None

        if 'return' in inputs:
            outputs = inputs['return']

        if 'return' in inputs:
            del inputs['return']

        # check inputs
        if len(self.fns) > 0:
            isprovided = True
            for _, t in inputs.items():
                isprovided = isprovided and t in self.provided_types
            assert(isprovided)

        self.fns.append(fn)

        if type(outputs) is tuple:
            for output in outputs:
                self.provided_types.append(output)
        else:
            self.provided_types.append(outputs)

    def __find_providers(self, inputs):
        fn_providers = []
        for _, input in inputs.items():
            fn_providers.append(self.providers[input])
        return fn_providers 

    def __args_signatures_valid(self, args, signatures):
        assert(len(args) == len(signatures))
        i : int = 0
        for _, arg_type in signatures.items():
            assert(type(args[i]) == arg_type)
    
    def __load_from_cache_or_exec(self, fn_outputs_signature, i, args):
        rtn = None
        iscached = True

        if type(fn_outputs_signature) == tuple:
            iscached = True
            for t in fn_outputs_signature:
                file_path = path.join(self.base_path, type(t).__name__)
                iscached = iscached and path.exists(file_path) and path.isfile(file_path)
        else: 
            file_path = path.join(self.base_path, type(fn_outputs_signature).__name__)
            iscached = path.exists(file_path) and path.isfile(file_path)
        
        if iscached:
            if type(fn_outputs_signature) == tuple:
                rtns = []
                for t in fn_outputs_signature:
                    file_path = path.join(self.base_path,type(t).__name__)
                    rtns.append(pickle.load(open(file_path, "rb")))
                rtn = tuple(rtns)
            else: 
                file_path = path.join("base_path", type(fn_outputs_signature).__name__)
                rtn = pickle.load(open(file_path, "rb"))
        else: 
            rtn = self.fns[i](*args)
        
        return rtn, iscached

    # TODO: Deal with code change detection using some hashing mechanism
    def run(self, *args):
        for i in range(len(self.fns)):
            iscached : bool = True
            fn = self.fns[i]
            rtn = None
            if i == 0:
                fn_inputs_signature = fn.__annotations__.copy()
                fn_outputs_signature = None
                if 'return' in fn_inputs_signature:
                    fn_outputs_signature = fn.__annotations__['return']
                    del fn_inputs_signature['return']
                
                self.__args_signatures_valid(args,fn_inputs_signature)

                rtn, iscached = self.__load_from_cache_or_exec(fn_outputs_signature,i,args)
            else:
                # find all of providers
                inputs = fn.__annotations__.copy()
                outputs = None
                if 'return' in inputs:
                    outputs = fn.__annotations__['return']
                    del inputs['return']

                rtn, iscached = self.__load_from_cache_or_exec(outputs, i, self.__find_providers(inputs))

            if type(rtn) is tuple:
                for r in rtn:
                    self.providers[type(r)] = r
                    if not iscached:
                        file_path = path.join(self.base_path, type(r).__name__)
                        pickle.dump(r, open(file_path, "wb"))
            else:
                self.providers[type(rtn)] = rtn
                if not iscached:
                    file_path = path.join(self.base_path, type(rtn).__name__)
                    pickle.dump(rtn, open(file_path, "wb"))
        return rtn
