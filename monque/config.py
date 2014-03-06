
import os, sys, logging

class Configuration(object):
    """
    Class for accessing configuration data, either stored in the DB or locally in this instance.
    Configuration can be inherited, which allows for a hierarchy from global config down to
    per-queue or even per-task.

    Configuration precedence:
    - Read from DB
    - Global config instance
    - Queue config params
    - Task class config params
    - Taks instance config params
    """
    def __init__(self,**kwargs):
        """
        Create a config instance, using the given parameters as initial config values
        """
        self.d = {}
        self.parent = kwargs.pop('parent',None)
        self.load(kwargs)

    def get(self,name,default=None):
        """
        Retrieve a config key, which can be specificied using dot-notation, e.g. foo.bar,
        which allows for nested config structure
        """
        val = self._get(name,self.d)
        if val is None:
            if self.parent:
                val = self.parent.get(name)
        if val is None:
            val = default
        return val

    def _get(self,name,d):
        try:
            return d[name]
        except KeyError:
            if '.' in name:
                part,rest = name.split('.',1)
                if part in d:
                    return self._get(rest,d[part])
            return None

    def set(self,name,val):
        d = self.d
        while '.' in name:
            part,rest = name.split('.',1)
            d[part] = {}
            d = d[part]
            name = rest
        d[name] = val

        return self


    def load(self,d):
        """
        Populate this config by loading from another dict object
        """
        for k,v in d.iteritems():
            self.set(k,v)

        return self

    def load_from_env(self):
        """
        Populate this config using environment variables.
        Looks for everything matching MONQUE_*
        Converts those all to config settings by:
        - stripping the MONQUE_ prefix
        - converting to lowercase
        - replacing _ with .

        For example: MONQUE_MONGO_HOST --> mongo.host
        """

        import os
        for k,v in os.environ.iteritems():
            if not k.startswith('MONQUE_'): continue
            cfg = k[7:].lower().replace('_','.')
            #print "environ[%s] --> %s = %s" % (k,cfg,v)
            self.set(cfg,v)

        return self

        
    @classmethod
    def get_global(klass,db,collection_name='config'):
        """
        Load the global configuation data for the given DB.
        The DB is expected to have a collection with the given name, which 
        contains a single document.
        """
        data = db[collection_name].find_one()
        if data:
            return klass().load(data)
        else:
            # No data found in the collection...
            log = logging.getLogger('monque')
            if 'log':
                log.warning("'%s' collection is empty, no global config settings" % 
                            (collection_name))
            return klass()


