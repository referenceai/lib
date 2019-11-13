class DictionaryRing():

    def reset(self):
        for k in self.pos.keys():
            self.pos[k] = 0
        for k,v in self.l.items():
            v.reverse()
            self.l[k] = v 

    def __getitem__(self, key):
        if key in self.l:
            r = self.l[key][self.pos[key]]
            self.pos[key] += 1
            return r
        else:
            return None

    def __setitem__(self, key, val):
        if key in self.l:
            self.l[key].insert(0, val)
        else:
            self.l[key] = [val]
        
        self.pos[key] = 0
    
    def __delitem__(self, key):
        del self.l[key]
        del self.pos[key]

    def items(self):
        it = []
        for k,v in self.l.items():
            it.append((k,v[0]))
        return iter(it)
    
    def __init__(self):
        self.l = {}
        self.pos = {}

    