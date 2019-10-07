class Pipeline():

    provided_types = []
    fns = []
    providers = {}

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

    # TODO: Add caching to each of the classes that are returned by the functions
    def run(self, *args):
        i: int = 0
        for fn in self.fns:
            rtn = None
            if i == 0:
                fn_inputs_signature = fn.__annotations__.copy()
                if 'return' in fn_inputs_signature:
                    del fn_inputs_signature['return']
                
                self.__args_signatures_valid(args,fn_inputs_signature)

                rtn = self.fns[0](*args) # first function takes in the inputs to the run function
            else:
                # find all of providers
                inputs = fn.__annotations__.copy()
                if 'return' in inputs:
                    del inputs['return']
                rtn = self.fns[i](*self.__find_providers(inputs))
            if type(rtn) is tuple:
                for r in rtn:
                    self.providers[type(r)] = r
            else:
                self.providers[type(rtn)] = rtn
            i += 1
        return rtn
