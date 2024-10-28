import os
from astropy.io import fits
from datetime import datetime, timedelta

def get_valid_calibration_range(science_date):
    """Returns the valid calibration time range based on the observation time of the science file."""
    obs_time = datetime.strptime(science_date, "%Y-%m-%dT%H:%M:%S.%f")
    
    if obs_time.hour >= 12:  # Science file observed between 12 PM and 12 AM
        cal_start = obs_time.replace(hour=12, minute=0, second=0)
        cal_end = cal_start + timedelta(days=1)
    else:  # Science file observed between 12 AM and 12 PM
        cal_end = obs_time.replace(hour=12, minute=0, second=0)
        cal_start = cal_end - timedelta(days=1)
    
    return cal_start, cal_end

# Example directory for test
download_dir = "."
files = os.listdir(download_dir)
sorted_files = sorted(files)

# Separate lists to log science and calibration files
science_files = []
cal_files = []

print("Starting file classification...")

for f in sorted_files:
    if 'fits' in f:
        fpath = os.path.join(download_dir, f)
        hdr = fits.open(fpath)[0].header
        date_str = hdr["DATE"]  # Observation timestamp in header
        obj = hdr["OBJECT"]
        
        if '.cat' in obj:  # Identify science files
            print(f"  Found science file: {f}")
            science_files.append((fpath, date_str, hdr["HIERARCH ESO OBS ID"]))
        else:  # Identify calibration files
            print(f"  Found calibration file: {f}")
            cal_files.append((fpath, date_str))

print("\nProcessing science files and associating calibration files...")

for science_file, sci_date_str, obid in science_files:
    obs_night = datetime.strptime(sci_date_str, "%Y-%m-%dT%H:%M:%S.%f").strftime("%Y-%m-%d")
    cal_range = get_valid_calibration_range(sci_date_str)  # Get valid time range for calibration files
    
    print(f"\nProcessing science file: {science_file}")
    print(f"  Science observation night: {obs_night}")
    print(f"  Valid calibration time range: {cal_range[0]} to {cal_range[1]}")
    
    # Logically associate calibration files that fall within this time range
    associated_cals = []
    
    for cal_file, cal_date_str in cal_files:
        cal_time = datetime.strptime(cal_date_str, "%Y-%m-%dT%H:%M:%S.%f")
        
        if cal_range[0] <= cal_time <= cal_range[1]:
            associated_cals.append(cal_file)
            print(f"  Calibration file {cal_file} is within the valid time range")
    
    if associated_cals:
        print(f"  Logically creating directory for science file: /{obs_night}/{obid}/science")
        print(f"  Logically moving science file {science_file} to /{obs_night}/{obid}/science")
        
        print(f"  Logically creating directory for calibration files: /{obs_night}/cal")
        for cal_file in associated_cals:
            print(f"  Logically moving calibration file {cal_file} to /{obs_night}/cal")
    else:
        print(f"  Warning: No associated calibration files found for science file {science_file}")

print("\nDone with file sorting simulation!")

