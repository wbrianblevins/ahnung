
import sys
import json
import getpass

import vehicle

#
# -- AhnungConfig
#
# Top-level configuration object.  Settings are read from a JSON file and
# are read-only.  Contains an AhnungVehicle for each configured estimator
# (ie. for each source collection name).
#
class AhnungConfig(object):
    """ Global configuration object for the ML pipeline.
    """

    GLOBAL_PROPERTIES   = 'global_properties'
    NUM_FOLDS           = 'num_folds'
    EST_LIST            = 'est_list'
    SRC_COLLNAME        = 'src_collname'
    TARGET_NAME         = 'target_name'
    IS_CLASSIFICATION   = 'is_classification'
    IS_REGRESSION       = 'is_regression'
    RANDOM_SEED         = 'random_seed'
    ALLOWED_CPUS        = 'allowed_cpus'
    MAX_GLOBAL_TIME     = 'max_global_time'
    MAX_PERMODEL_TIME   = 'max_permodel_time'

    SCHEMA_PROPERTIES   = 'schema_properties'
    AT_MIN_PRESENT      = 'attr_type_min_present'
    AT_MIN_TYPEALIGN    = 'attr_type_min_typealign'
    MAX_CAT_VALS        = 'max_categorical_values'

    MODEL_PROPERTIES    = 'model_properties'
    CATEGORY_BALANCING  = 'target_category_balancing'
    BALANCE_CAT_NONE    = 'none'
    BALANCE_CAT_EQUAL   = 'equalize'
    BALANCE_CAT_AVG     = 'average'
    CATEGORY_MAX_OVER   = 'category_max_oversample'

    SERVICE_PROPERTIES  = 'service_properties'
    SERVICE_HOSTNAME    = 'service_hostname'
    SERVICE_PORT        = 'service_port'

    CONNECT_URIS        = 'connect_uris'
    SOURCE_URI          = 'source_uri'
    RAWDOCS_URI         = 'rawdocs_uri'
    METADATA_URI        = 'metadata_uri'
    CLEANED_URI         = 'cleaned_uri'
    # FOLDS_URI           = 'folds_uri'
    # RESULTS_URI         = 'results_uri'
    CREDS_PLACEHOLDER   = 'usercredsplaceholder'

    DEF_RANDOM_SEED     = 10001
    DEF_ALLOWED_CPUS    = 1
    DEF_MAX_GLOBAL_TIME = 600
    DEF_PERMODEL_TIME   = 60
    DEF_SERV_HOSTNAME   = 'localhost'
    DEF_SERV_PORTNUM    = 8088

    #
    #
    #
    def loadCredentials(self):
        promptStr = '\nEnter the MongoDB database username.\n'
        self.username = input(promptStr)
        promptStr = '\nEnter the MongoDB database password.\n'
        self.password = getpass.getpass(prompt=promptStr, stream=None)


    #
    #
    #
    def getUsername(self):

        if None == self.username:
            self.loadCredentials()

        return self.username


    #
    #
    #
    def getPassword(self):

        if None == self.password:
            self.loadCredentials()

        return self.password


    #
    #
    #
    def isStringTrue(self, testStr):
        return testStr in ['True', 'TRUE', 'true', '1', 'Yes', 'YES', 'yes', 'On', 'ON', 'on']

    
    #
    # Constructor loads the settings from the provided filename.
    #
    def __init__(self, filename):
        """ Constructor loads and parses the JSON configuration file.
        """

        self.username = None
        self.password = None

        with open(filename) as fd:
            try:
                self.settings = json.load(fd)
            finally:
                fd.close()
            
        # Must come after the settings load from disk.
        self.vehicleDict = self.makeDefaultVehicleDict()



    #
    # Instantiate empty/default vehicle objects for each configured estimator.
    #
    def makeDefaultVehicleDict(self):
        estList = self.getEstimatorList()
        vDict = {}

        for estimator in estList:

            estName = estimator.get(self.SRC_COLLNAME)
            target = estimator.get(self.TARGET_NAME)

            vDict[estName] = vehicle.AhnungVehicle(estName, target, self)
            
        return vDict


    #
    #
    #
    def getEstVehicle(self, estName):
        rVehicle = None
        if None != estName:
            rVehicle = self.vehicleDict[estName]
        return rVehicle



    #
    #
    #
    def getConnectURIDict(self):
        uri_dict = self.settings[self.CONNECT_URIS]
        return uri_dict


    def getGlobalPropertiesDict(self):
        props_dict = self.settings.get(self.GLOBAL_PROPERTIES)
        return props_dict


    def getSchemaPropertiesDict(self):
        props_dict = self.settings.get(self.SCHEMA_PROPERTIES)
        return props_dict


    def getModelPropertiesDict(self):
        props_dict = self.settings.get(self.MODEL_PROPERTIES)
        return props_dict


    def getServicePropertiesDict(self):
        props_dict = self.settings.get(self.SERVICE_PROPERTIES)
        return props_dict


    def getSourceURI(self):

        user = self.getUsername()
        pwd  = self.getPassword()
        cred = user + ':' + pwd

        uri_dict = self.getConnectURIDict()
        work_uri = uri_dict.get(self.SOURCE_URI)
        work_uri = work_uri.replace(self.CREDS_PLACEHOLDER, cred, 1)

        return work_uri


    def getRawDocsURI(self):

        user = self.getUsername()
        pwd  = self.getPassword()
        cred = user + ':' + pwd

        uri_dict = self.getConnectURIDict()
        work_uri = uri_dict.get(self.RAWDOCS_URI)
        work_uri = work_uri.replace(self.CREDS_PLACEHOLDER, cred, 1)

        return work_uri


    def getMetaDataURI(self):

        user = self.getUsername()
        pwd  = self.getPassword()
        cred = user + ':' + pwd

        uri_dict = self.getConnectURIDict()
        work_uri = uri_dict.get(self.METADATA_URI)
        work_uri = work_uri.replace(self.CREDS_PLACEHOLDER, cred, 1)

        return work_uri


    def getCleanedURI(self):

        user = self.getUsername()
        pwd  = self.getPassword()
        cred = user + ':' + pwd

        uri_dict = self.getConnectURIDict()
        work_uri = uri_dict.get(self.CLEANED_URI)
        work_uri = work_uri.replace(self.CREDS_PLACEHOLDER, cred, 1)

        return work_uri


    def getFoldsURI(self):

        user = self.getUsername()
        pwd  = self.getPassword()
        cred = user + ':' + pwd

        uri_dict = self.getConnectURIDict()
        work_uri = uri_dict.get(self.FOLDS_URI)
        work_uri = work_uri.replace(self.CREDS_PLACEHOLDER, cred, 1)

        return work_uri


    def getResultsURI(self):

        user = self.getUsername()
        pwd  = self.getPassword()
        cred = user + ':' + pwd

        uri_dict = self.getConnectURIDict()
        work_uri = uri_dict.get(self.RESULTS_URI)
        work_uri = work_uri.replace(self.CREDS_PLACEHOLDER, cred, 1)

        return work_uri


    #
    #
    #
    def getEstimatorList(self):

        gProps_dict = self.getGlobalPropertiesDict()
        est_list = gProps_dict.get(self.EST_LIST)

        return est_list


    #
    #
    #
    def getEstimatorByName(self, estName):

        estDict = None
        est_list = self.getEstimatorList()
        
        for estimator in est_list:
            if estName == estimator.get(self.SRC_COLLNAME):
                estDict = estimator
                break

        return estDict


    #
    #
    #
    def getEstimatorBooleanFlag(self, estName, boolFlagName):
        estDict = self.getEstimatorByName(estName)
        flagStr = estDict[boolFlagName]
        flagBool = self.isStringTrue(flagStr)
        return flagBool


    #
    #
    #
    def getEstimatorInteger(self, estName, integerName, defaultVal=0):
        intResult = defaultVal
        estDict = self.getEstimatorByName(estName)
        intStr = None
        if None != estDict:
            intStr = estDict.get(integerName)
        if None != intStr:
            intResult = int(intStr)
        return intResult


    #
    #
    #
    def getRandomSeed(self):

        rand_seed = self.DEF_RANDOM_SEED
        
        gProp_dict = self.getGlobalPropertiesDict()
        if None != gProp_dict:
            rand_seed_str = gProp_dict.get(self.RANDOM_SEED)
            if None != rand_seed_str:
                rand_seed = int(rand_seed_str)

        return rand_seed



    #
    #
    #
    def getAllowedCPUs(self):

        allow_cpus = self.DEF_ALLOWED_CPUS
        
        gProp_dict = self.getGlobalPropertiesDict()
        if None != gProp_dict:
            allow_cpus_str = gProp_dict.get(self.ALLOWED_CPUS)
            if None != allow_cpus_str:
                allow_cpus = int(allow_cpus_str)

        return allow_cpus



    #
    #
    #
    def getMaxGlobalTime(self):

        max_global = self.DEF_MAX_GLOBAL_TIME
        
        gProp_dict = self.getGlobalPropertiesDict()
        if None != gProp_dict:
            max_global_str = gProp_dict.get(self.MAX_GLOBAL_TIME)
            if None != max_global_str:
                max_global = int(max_global_str)

        return max_global



    #
    #
    #
    def getMaxPerModelTime(self):

        max_permodel = self.DEF_PERMODEL_TIME
        
        gProp_dict = self.getGlobalPropertiesDict()
        if None != gProp_dict:
            max_permodel_str = gProp_dict.get(self.MAX_PERMODEL_TIME)
            if None != max_permodel_str:
                max_permodel = int(max_permodel_str)

        return max_permodel



    #
    #
    #
    def getServiceHostname(self):

        serv_hostname = self.DEF_SERV_HOSTNAME
        
        gServ_dict = self.getServicePropertiesDict()
        if None != gServ_dict:
            serv_hostname_str = gServ_dict.get(self.SERVICE_HOSTNAME)
            if None != serv_hostname_str:
                serv_hostname = serv_hostname_str

        return serv_hostname


    #
    #
    #
    def getServicePort(self):

        serv_port = self.DEF_SERV_PORTNUM
        
        gServ_dict = self.getServicePropertiesDict()
        if None != gServ_dict:
            serv_port_str = gServ_dict.get(self.SERVICE_PORT)
            if None != serv_port_str:
                serv_port = int(serv_port_str)

        return serv_port



    #
    #
    #
    def getSchemaAttrMinPresent(self):

        at_mp = 0.8
        
        schema_dict = self.getSchemaPropertiesDict()
        if None != schema_dict:
            at_mp_str = schema_dict.get(self.AT_MIN_PRESENT)
            if None != at_mp_str:
                at_mp = float(at_mp_str)

        return at_mp


    #
    #
    #
    def getSchemaAttrMinTypeAlignment(self):

        at_mpt = 0.8
        
        schema_dict = self.getSchemaPropertiesDict()
        if None != schema_dict:
            at_mpt_str = schema_dict.get(self.AT_MIN_TYPEALIGN)
            if None != at_mpt_str:
                at_mpt = float(at_mpt_str)

        return at_mpt


    #
    #
    #
    def getSchemaMaxCategoricalValues(self):

        max_vals = 10
        
        schema_dict = self.getSchemaPropertiesDict()
        if None != schema_dict:
            max_vals_str = schema_dict.get(self.MAX_CAT_VALS)
            if None != max_vals_str:
                max_vals = int(max_vals_str)

        return max_vals


    #
    #
    #
    def getModelCategoryBalancing(self):

        bal_flag = self.BALANCE_CAT_NONE
        
        model_dict = self.getModelPropertiesDict()
        if None != model_dict:
            balance_str = model_dict.get(self.CATEGORY_BALANCING)
            if None != balance_str:
                bal_flag = str(balance_str)

        return bal_flag


    #
    #
    #
    def getModelMaxOversample(self):

        max_over = 2.0
        
        model_dict = self.getModelPropertiesDict()
        if None != model_dict:
            maxover_str = model_dict.get(self.CATEGORY_MAX_OVER)
            if None != maxover_str:
                max_over = float(maxover_str)

        return max_over





