import pickle
from os import path, makedirs

class Pipeline():

    provided_types = []
    fns = []
    providers = {}

    def __init__(self):
        makedirs(path.join(".rai/cache"))

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

    # TODO: Deal with code change detection using some hashing mechanism
    def run(self, *args):
        i: int = 0
        for fn in self.fns:
            rtn = None
            if i == 0:
                fn_inputs_signature = fn.__annotations__.copy()
                fn_outputs_signature = None
                if 'return' in fn_inputs_signature:
                    fn_outputs_signature = fn.__annotations__['return']
                    del fn_inputs_signature['return']
                
                self.__args_signatures_valid(args,fn_inputs_signature)

                # start of caching code
                iscached = True
                if type(fn_outputs_signature) == tuple:
                    iscached = True
                    for t in fn_outputs_signature:
                        iscached = iscached and path.exists(".rai/cache/" + str(type(t))) and path.isfile(".rai/cache/" + str(type(t)))
                else: 
                    iscached = path.exists(".rai/cache/" + str(type(fn_outputs_signature))) and path.isfile(".rai/cache/" + str(type(t)))
                
                if iscached:
                    if type(fn_outputs_signature) == tuple:
                        rtns = []
                        for t in fn_outputs_signature:
                            rtns.append(pickle.load(".rai/cache/" + str(type(t)), "rb"))
                        rtn = tuple(rtns)
                    else: 
                        rtn = pickle.load(".rai/cache/" + str(type(fn_outputs_signature)), "rb")
                else: 
                    # end of caching codes
                    rtn = self.fns[0](*args) # first function takes in the inputs to the run function
            else:
                # find all of providers
                inputs = fn.__annotations__.copy()
                outputs = None
                if 'return' in inputs:
                    outputs = fn.__annotations__['return']
                    del inputs['return']

                # start of caching code
                iscached = True
                if type(outputs) == tuple:
                    iscached = True
                    for t in outputs:
                        iscached = iscached and path.exists(".rai/cache/" + type(t)) and path.isfile(".rai/cache/" + type(t))
                else: 
                    iscached = path.exists(".rai/cache/" + type(outputs)) and path.isfile(".rai/cache/" + type(t))
                
                if iscached:
                    if type(outputs) == tuple:
                        rtns = []
                        for t in outputs:
                            rtns.append(pickle.load(".rai/cache/" + type(t), "rb"))
                        rtn = tuple(rtns)
                    else: 
                        rtn = pickle.load(".rai/cache/" + type(outputs), "rb")
                else: 
                    # end of caching codes                
                    rtn = self.fns[i](*self.__find_providers(inputs))

            if type(rtn) is tuple:
                for r in rtn:
                    self.providers[type(r)] = r
                    if not iscached:
                        pickle.dump(r, open(".rai/cache/" + str(type(r))))
            else:
                self.providers[type(rtn)] = rtn
                if not iscached:
                    print(".rai/cache/" + str(type(rtn)))
                    pickle.dump(rtn, open(".rai/cache/" + str(type(rtn))))
            i += 1
        return rtn
