import pickle
import os
from os import path, makedirs
import shutil
from loguru import logger
from abc import abstractmethod

class Pipeline():

    base_path = None

    def __init__(self, id, base_path = None):
        self.id = id
        if base_path is None:
            self.base_path = path.join(os.getcwd(), ".rai", "cache", self.id)
            logger.debug("Base path for cache set to " + str(base_path))
        if not path.exists(self.base_path):
            makedirs(self.base_path)
            logger.debug("Create base path at " + str(base_path))
        # Instance variables intialization
        self.provided_types = []
        self.fns = []
        self.providers = {}
    
    @logger.catch
    def expunge_cache(self):
        shutil.rmtree(self.base_path)
        makedirs(self.base_path)
        
    @logger.catch
    def push(self, fn):
        # transform must be a function
        assert(callable(fn))
        # now we are going to verify the inputs
        # and make sure that something in the pipeline
        # is ready to supply it
        
        inputs = fn.__annotations__.copy()
        outputs = None

        assert(len(inputs) > 0) # This module will not work without annotations

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
            i += 1
    
    def __load_from_cache_or_exec(self, fn_outputs_signature, i, args):
        rtn = None
        iscached = True
        base_path = path.join(self.base_path, str(i))
        wfn = lambda: self.fns[i](*args)

        if not path.exists(base_path):
            makedirs(base_path)

        if type(fn_outputs_signature) == tuple:
            iscached = True
            for j in range(len(fn_outputs_signature)):
                file_path = path.join(base_path, str(j))
                iscached = iscached and path.exists(file_path) and path.isfile(file_path)
        else: 
            file_path = path.join(base_path, "0")
            iscached = path.exists(file_path) and path.isfile(file_path)
        
        if iscached:
            # verify that the signature of the functions are correct
            file_hash = open(path.join(base_path,"obj"), "r")
            hash_value = file_hash.readline()
            if hash_value == hash(wfn):
                if type(fn_outputs_signature) is not tuple:
                    fn_outputs_signature = tuple([fn_outputs_signature])
                rtns = []
                for j in range(len(fn_outputs_signature)):
                    file_path = path.join(base_path, str(j))
                    rtns.append(pickle.load(open(file_path, "rb")))
                rtn = tuple(rtns)
            else:
                iscached = False
        
        if not iscached:
            rtn = wfn()
        
        return rtn, iscached

    # TODO: We must detect if any classes that functions depend on have changed
    @logger.catch
    def run(self, *args):
        rtn = None
        for i in range(len(self.fns)):
            iscached : bool = True
            fn = self.fns[i]
            fn_args = None
            fn_inputs_signature = fn.__annotations__.copy()
            fn_outputs_signature = None
            
            if 'return' in fn_inputs_signature:
                fn_outputs_signature = fn.__annotations__['return']
                del fn_inputs_signature['return']

            if i == 0:
                fn_args = args
                self.__args_signatures_valid(args,fn_inputs_signature)
            else:
                fn_args = self.__find_providers(fn_inputs_signature)

            rtn, iscached = self.__load_from_cache_or_exec(fn_outputs_signature, i, fn_args)
            logger.info("Loaded function " + str(i) + " from cache at " + self.base_path)

            if type(rtn) is not tuple:
                rtn = tuple([rtn])

            for j in range(len(rtn)):
                r = rtn[j]
                self.providers[type(r)] = r
                if not iscached:
                    base_path = path.join(self.base_path, str(i))
                    # serialized return
                    file_path = path.join(base_path, str(j))
                    pickle.dump(r, open(file_path, "wb"))
                    # function hash with parameters (a colosure with all parameters already set)
                    wfn = lambda: fn(*fn_args)
                    file_hash = open(path.join(base_path, "obj"), "w")
                    file_hash.write(str(hash(wfn)))
                    file_hash.close()
                    
                    
        if len(rtn) == 1:
            rtn = rtn[0]
        return rtn

class PipelineAIProvider():
    @abstractmethod
    def hyperparameters(self):
        pass

    @abstractmethod
    def optimizer(self):
        pass

    @abstractmethod
    def datasource(self):
        pass

    @abstractmethod
    def transform(self):
        pass

    @abstractmethod
    def train(self):
        pass

    @abstractmethod
    def save(self):
        pass

    @abstractmethod
    def load(self):
        pass


class PipelineAI(Pipeline):

    def __build_pipeline(self, id: str, fns) -> Pipeline:
        p = Pipeline(id)
        map(lambda m: p.push(m), fns)
        return p

    def __init__(self, provider: PipelineAIProvider):
        self.provider = provider
        self.train_pipeline = self.__build_pipeline("train",
                      [self.provider.hyperparameters(),
                       self.provider.optimizer(),
                       self.provider.datasource(),
                       self.provider.transform(),
                       self.provider.train(),
                       self.provider.save()])

        self.classify_pipeline = self.__build_pipeline("classify",
                      [self.provider.hyperparameters(),
                       self.provider.load(),
                       self.provider.classify()])

        self.update_pipeline = self.__build_pipeline("update",
                      [self.provider.hyperparameters(),
                       self.provider.optimizer(),
                       self.provider.datasource(),
                       self.provider.updatesource(),
                       self.provider.transform(),
                       self.provider.train(),
                       self.provider.save()])

        self.update_bulk_pipeline = self.__build_pipeline("update_bulk",
                      [self.provider.hyperparameters(),
                       self.provider.optimizer(),
                       self.provider.datasource(),
                       self.provider.updatesource_bulk(),
                       self.provider.transform(),
                       self.provider.train(),
                       self.provider.save()])

    def train(self):
        return self.train_pipeline.run()

    def classify(self, obj):
        return self.classify_pipeline.run()

    def update(self, obj):
        return self.update_pipeline.run()

    def update_bulk(self, objs):
        return self.update_bulk_pipeline.run()
