import sshtunnel
import pymongo
from time import time, strftime, gmtime
from os import path, scandir, environ
from sys import argv
from json import loads as decodeJSON
argv = argv[1:]


# FUNCIONES
def setEnv(chainText):
    for k, v in {
            'timestamp': int(time())
            }.items():
        chainText = chainText.replace(f'@{k}', str(v))
    if chainText[:2] == '${' and chainText[-1] == '}':
        chainText = eval(chainText[2:-1])
    elif chainText[0] == '$' and chainText[1:] in environ.keys():
        chainText = environ[chainText[1:]]
    return chainText


def safeComplexity(obj):
    def recursiveKeys(obj, ret=[], prefix='', d='.'):
        if type(obj) == dict:
            for k, x in obj.items():
                if type(x) == dict:
                    ret = [*ret, *recursiveKeys(x, ret=[], prefix=f'{prefix}{k}{d}')]
                else:
                    ret.append(f'{prefix}{k}')
        return ret

    def fget(key, obj, d='.'):
        key, tmp = key.split(d), obj
        for k in key:
            if k in tmp:
                tmp = tmp[k]
            else:
                break
        return tmp
    return {k: fget(k, obj) for k in recursiveKeys(obj)}


def dictDeconstruction(x):
    def inWord(word, let, caseSensitive=False):
        ret = False
        for x in word:
            if (x.lower() == let.lower() if caseSensitive else x == let):
                ret = not ret
                break
        return ret
    ret = {}
    if type(x) == dict:
        for k, v in x.items():
            if inWord(k, '.'):
                tmp = last = ''
                for q in k.split('.'):
                    tmp += f'[\'{q}\']'
                    if eval(f'type(ret{last}) == dict and not \'{q}\' in ret{last}.keys()'):
                        exec(f'r{tmp}=' + '{}', {'r': ret})
                    last = tmp
                exec(f'ret{tmp} = v', {'ret': ret, 'v': v})
            else:
                ret[k] = v
    return ret


# VALIDACION
if len(argv) >= 1:
    fileConfName = f'./conf/{argv[0].lower()}.json'.replace('//', '/')
    if path.isfile(fileConfName):
        conf = decodeJSON(open(fileConfName, encoding='utf8').read())
        invalidValues = [z[0] for z in filter(lambda x: x[1] is None, conf.items())]
        entvars = {z[0]: z[1] if not z[1].isnumeric() else int(z[1]) for z in [x.split('=') for x in filter(lambda q: '=' in q, argv[1:])]}
        conf = {**conf, **entvars}
        conf = dictDeconstruction({k: v if type(v) != str else setEnv(v) for k, v in safeComplexity(conf).items()})
        if len(invalidValues) > 0:
            print(f'The follow params don\'t be null: {",".join(invalidValues)}')
            exit()
    else:
        print(f'Profile {argv[0]} not found.')
else:
    print(f"Profile name is required. Select profile: \n{chr(10).join([f' - {path.basename(x.path)[:-5]}' for x in scandir('./conf')])}")
    exit()

# Asignaciones
SSH_SERVER = conf['SSH_SERVER']
SSH_PORT = conf['SSH_PORT']
SSH_USERNAME = conf['SSH_USERNAME']
SSH_CERT = conf['SSH_CERT']
REMOTE_MONGO_USER = conf['REMOTE_MONGO_USER']
REMOTE_MONGO_PASS = conf['REMOTE_MONGO_PASS']
REMOTE_MONGO_DB = conf['REMOTE_MONGO_DB']
REMOTE_MONGO_COLLECTION = conf['REMOTE_MONGO_COLLECTION']
LOCAL_BIND_ADDRESS = conf['LOCAL_BIND_ADDRESS']
LOCAL_BIND_PORT = conf['LOCAL_BIND_PORT']
LOCAL_MONGO_USER = conf['LOCAL_MONGO_USER']
LOCAL_MONGO_PASS = conf['LOCAL_MONGO_PASS']
LOCAL_MONGO_DB = conf['LOCAL_MONGO_DB']
LOCAL_MONGO_HOST = conf['LOCAL_MONGO_HOST']
LOCAL_MONGO_PORT = conf['LOCAL_MONGO_PORT']
QUERY_MATCH = conf['QUERY_MATCH']
MONGO_URI = f'mongodb://{REMOTE_MONGO_USER}:{REMOTE_MONGO_PASS}@{LOCAL_BIND_ADDRESS}:{LOCAL_BIND_PORT}'
db = pymongo.MongoClient(f'mongodb://{LOCAL_MONGO_USER}:{LOCAL_MONGO_PASS}@{LOCAL_MONGO_HOST}:{LOCAL_MONGO_PORT}/')[LOCAL_MONGO_DB]


def getData(collection_name, query={}):
    # define ssh tunnel
    server = sshtunnel.SSHTunnelForwarder(
        SSH_SERVER,
        ssh_username=SSH_USERNAME,
        ssh_pkey=SSH_CERT,
        ssh_port=SSH_PORT,
        remote_bind_address=(LOCAL_BIND_ADDRESS, LOCAL_BIND_PORT)
    )
    server.start()
    connection = pymongo.MongoClient(host=LOCAL_BIND_ADDRESS,
                                     port=server.local_bind_port,
                                     username=REMOTE_MONGO_USER,
                                     password=REMOTE_MONGO_PASS
                                     )
    db = connection[REMOTE_MONGO_DB]
    data = db[collection_name].find(query)
    return data


errorFileContent = []
counterIns, counterErr, total = 0, 0, 0
for entry in getData(REMOTE_MONGO_COLLECTION, QUERY_MATCH):
    entry['dateInput'] = int(time())
    try:
        db.customers.insert_one(entry)
        counterIns += 1
    except Exception as e:
        errorFileContent.append(str(e))
        counterErr += 1
    total += 1
GMT = -3  # TIMEZONE
date = gmtime(time()+(GMT*3600))
# Crear archivos si no existen
if not path.isfile('./logs/current_execution.log'):
    tmpFile = open('./logs/current_execution.log', 'w+')
    tmpFile.write(f"{' FROM DATABASE PRODUCTION '.center(107,'/')}\n{'Date'.ljust(40, ' ')}{'Collection'.ljust(17, ' ')}{'Total match'.ljust(17, ' ')}{'Inserted'.ljust(17, ' ')}{'No inserted'.ljust(17, ' ')}\n")
    tmpFile.close()
if not path.isfile('./logs/current_errors.log'):
    tmpFile = open('./logs/current_errors.log', 'w+')
    tmpFile.close()
# header = f"{' FROM DATABASE PRODUCTION '.center(107,'/')}\n{'Date'.ljust(40, ' ')}{'Collection'.ljust(17, ' ')}{'Total match'.ljust(17, ' ')}{'Inserted'.ljust(17, ' ')}{'Inserted'.ljust(17, ' ')}{'No inserted'.ljust(17, ' ')}\n"
print(f'{strftime(f"%d-%m-%Y %H:%M:%S GMT{GMT}", date)}[{int(time())}]   {REMOTE_MONGO_COLLECTION.ljust(17, " ")}{str(total).ljust(17, " ")}{str(counterIns).ljust(17, " ")}{str(counterErr).ljust(17, " ")}')
tmpFile = open('./logs/current_execution.log', 'a+')
tmpFile.write(f'{strftime(f"%d-%m-%Y %H:%M:%S GMT{GMT}", date)}[{int(time())}]   {REMOTE_MONGO_COLLECTION.ljust(17, " ")}{str(total).ljust(17, " ")}{str(counterIns).ljust(17, " ")}{str(counterErr).ljust(17, " ")}\n')
tmpFile.close()
tmpFile = open('./logs/current_errors.log', 'a+')
for x in errorFileContent:
    tmpFile.write(f'{x}\n')
tmpFile.close()
