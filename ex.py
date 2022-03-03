import os
import sys
import hdf5_getters
import numpy as np
from pymongo import MongoClient


def ex_dir(songs):
    if len(sys.argv) < 2:
        print('USE:\npython3 ex.py -dir <DIR>')
        sys.exit(0)
    dir_ = sys.argv[1]
    if not os.path.isdir(dir_):
        print('ERROR: dir',dir_,'does not exist.')
        sys.exit(0)
    files = [os.path.join(dir_,f) for f in os.listdir(dir_) if os.path.isfile(os.path.join(dir_, f))]
    toUpload = []
    for i,f in enumerate(files):
        print(f'Processing: {f}\n')
        toUpload.append(extract(f, 0, ''))

    upload(toUpload, songs)
    print(f'------------ Processed {i} files ----------------')

def main():
    dbName = 'deproject'
    collName = 'songs'
    mongo_client = MongoClient('mongodb://localhost:27017')
    db = mongo_client[dbName]
    songs = db[collName]
    # help menu
    if len(sys.argv) < 2:
        print('USE:\npython3 ex.py [FLAGS] <HDF5 file> <OPT: song idx> <OPT: getter>')
        sys.exit(0)

    # flags
    summary = False
    while True:
        if sys.argv[1] == '-dir':
            sys.argv.pop(1)
            ex_dir(songs)
            sys.exit(0)

        if sys.argv[1] == '-summary':
            summary = True
        else:
            break
        sys.argv.pop(1)

    # get params
    hdf5path = sys.argv[1]
    songidx = 0
    if len(sys.argv) > 2:
        songidx = int(sys.argv[2])
    onegetter = ''
    if len(sys.argv) > 3:
        onegetter = sys.argv[3]

    toUpload = extract(hdf5path, songidx, onegetter)
    upload([toUpload], songs)

def extract(hdf5path, songidx, onegetter):
    # sanity check
    if not os.path.isfile(hdf5path):
        print('ERROR: file',hdf5path,'does not exist.')
        sys.exit(0)
    h5 = hdf5_getters.open_h5_file_read(hdf5path)
    numSongs = hdf5_getters.get_num_songs(h5)
    if songidx >= numSongs:
        print('ERROR: file contains only',numSongs)
        h5.close()
        sys.exit(0)

    # get all getters
    getters = list(filter(lambda x: x[:4] == 'get_', hdf5_getters.__dict__.keys()))
    getters.remove("get_num_songs") # special case
    if onegetter == 'num_songs' or onegetter == 'get_num_songs':
        getters = []
    elif onegetter != '':
        if onegetter[:4] != 'get_':
            onegetter = 'get_' + onegetter
        try:
            getters.index(onegetter)
        except ValueError:
            print('ERROR: getter requested:',onegetter,'does not exist.')
            h5.close()
            sys.exit(0)
        getters = [onegetter]
    getters = np.sort(getters)

    dict_ = {}
    # print them
    for getter in getters:
        try:
            res = hdf5_getters.__getattribute__(getter)(h5,songidx)
        except AttributeError as e:
            if summary:
                continue
            else:
                print(e)
                print('forgot -summary flag? specified wrong getter?')
        if res.__class__.__name__ == 'ndarray':
            #print(getter[4:]+": shape =",res.shape)
            dict_[getter[4:]] = res.shape
        else:
            #print(getter[4:]+":",res)
            if (res.__class__.__name__ == 'int32'):
                dict_[getter[4:]] = int(res)
                continue
            if (res.__class__.__name__ == 'bytes_'):
                dict_[getter[4:]] = str(res)
                continue
            dict_[getter[4:]] = res
    # done
    #print('DONE, showed song',songidx,'/',numSongs-1,'in file:',hdf5path)
    h5.close()
    return dict_


def upload(d, songs):
    results = songs.insert_many(d)
    print(f'UPLOADED: {results}')

if __name__ == "__main__":
    print('MAIN\n')
    main()
    
