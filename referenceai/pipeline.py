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

    def __find_providers(self,inputs):
        fn_providers = []
        for _, input in inputs.items():
            fn_providers.append(self.providers[input])
        return fn_providers 

    def run(self, *args):
        i: int = 0
        for fn in self.fns:
            rtn = None
            if i == 0:
                # TODO: must check that all args corresponds with the types in function call
                #       although perhaps it is a non-issue considering that this is taking
                #       place at runtime, and if they differ it will definitely fail
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
