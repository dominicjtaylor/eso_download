import os
import shutil
from astropy.io import fits
from datetime import datetime, timedelta

# Define the directory containing the files
download_dir = '.'  # Change to your source directory

print('\nDecompressing files')
os.system(f'parallel uncompress ::: {download_dir}/*.Z')

files = os.listdir(download_dir)
sorted_files = sorted(files)

# Separate lists to log science and calibration files
science_files = []
cal_files = []
obids = []

print("\nProcessing science files and associating calibration files (18:00d1-18:00d2)")
for f in sorted_files:
    if 'fits' in f:
        fpath = os.path.join(download_dir, f)
        if not os.path.exists(fpath):
            print(f"Error: File {fpath} does not exist.")
            continue
        hdr = fits.open(fpath)[0].header
        date_str = hdr["DATE"]  # Observation timestamp in header
        obj = hdr["OBJECT"]
        if '.cat' in obj:  # Identify science files
            science_files.append(fpath)
            obid_str = str(hdr["HIERARCH ESO OBS ID"])
            obids.append(obid_str)            
        else:  # Identify calibration files
            cal_files.append(fpath)

#Organise science files
print('Found the following nights:')
nights = []
for fndx, f in enumerate(science_files):
    fpath = os.path.join(download_dir, f)
    hdr = fits.open(fpath)[0].header
    date_str = hdr["DATE"]
    obs_datetime = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f") # Parse the timestamp
    obs_date = obs_datetime.strftime("%Y-%m-%d")

    # print(f,obids[fndx])
    # Determine the night directory
    if fndx == 0:
        if obs_datetime.hour < 18:
            # After midnight but before 18:00
            night_str = (obs_datetime - timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            # Before midnight but after 18:00
            night_str = obs_date

    else:
        if obids[fndx] == obids[fndx-1] and \
                datetime.strptime(obs_date, "%Y-%m-%d") == datetime.strptime(nights[fndx-1], "%Y-%m-%d") + timedelta(days=1): #If this is not first science obs of the OBID
            night_str = nights[fndx-1]
        else: #If this is first science obs of OBID
            if obs_datetime.hour < 18:
                # After midnight but before 18:00
                night_str = (obs_datetime - timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                # Before midnight but after 18:00
                night_str = obs_date
    print('Before:',obs_date,' After:',night_str)

    if night_str not in nights:
        print(night_str)
    nights.append(night_str)

    # Create directory structure for the science file
    science_dir = os.path.join(download_dir, night_str, obids[fndx], 'science')
    os.makedirs(science_dir, exist_ok=True)

    # Move the science file to the appropriate directory
    shutil.move(fpath, science_dir)
    # print(f'Moving science file {f} to {science_dir}')

#Organise cal files
for fndx, f in enumerate(cal_files):
    fpath = os.path.join(download_dir, f)
    hdr = fits.open(fpath)[0].header
    date_str = hdr["DATE"]
    obs_datetime = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f") # Parse the timestamp
    obs_date = obs_datetime.strftime("%Y-%m-%d")

    # Determine the night directory
    if obs_datetime.hour < 18:
        # After midnight but before 18:00
        night_str = (obs_datetime - timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        # Before midnight but after 18:00
        night_str = obs_date
    print('Before:',obs_date,' After:',night_str)

    # Create directory structure for the science file
    cal_dir = os.path.join(download_dir, night_str, 'cal')
    os.makedirs(cal_dir, exist_ok=True)

    # Move the science file to the appropriate directory
    shutil.move(fpath, cal_dir)
    # print(f'Moving cal file {f} to {cal_dir}')
