import os
import numpy as np
from astropy.io import fits

def make_tree()

    files = os.listdir('.')
    sorted_files = sorted(files)
    dates = []
    for f in sorted_files:
        if 'fits' in f:
            print(f)
            fpath = os.path.join(file_dir,f)
            hdr = fits.open(fpath)[0].header
            d = hdr["DATE"].split('T')[0]
            dates.append(d)

            date_dir = os.path.join(file_dir,d)
            os.system('mkdir -p '+date_dir)
            os.system('mv '+fpath+' '+date_dir)

    #In each date directory, make OBID directories or cal directories and move files
    for d in np.unique(dates):
        date_dir = os.path.join(file_dir,d)
        for f in os.listdir(date_dir): #files in date directory
            if 'fits' in f:
                fpath = os.path.join(date_dir,f)
                hdr = fits.open(fpath)[0].header
                obj = hdr["OBJECT"]

                #If science file, make OBID/science and move file there
                if '.cat' in obj:
                    obid = hdr["HIERARCH ESO OBS ID"]
                    obid_path = os.path.join(date_dir,obid)
                    os.system('mkdir -p '+obid_path+'/science')
                    os.system('mv '+fpath+' '+obid_path+'/science')

                else:
                    cal_path = os.path.join(date_dir,'cal')
                    os.system('mkdir -p '+cal_path)
                    os.system('mv '+fpath+' '+cal_path)
