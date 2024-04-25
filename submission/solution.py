import pandas as pd
import re

# Fastest runtime: 10.78 seconds

def stratified_random_sample(filepath):
    # Load CSV file
    df = pd.read_csv(filepath)

    # Store original columns
    cols = df.columns

    # Drop rows that do not contain enough information.

    # Strata Level 1
    df.dropna(subset=['AudioMothCode'], how='all', inplace=True)
    df.dropna(subset=['Duration', 'FileSize'], how='all', inplace=True)

    # Strata Level 2
    df.dropna(subset=['StartDateTime', 'Comment'], how='all', inplace=True)

    # Convert file size from bytes to megabytes
    df['FileSize'] = (df['FileSize'] / 10**6).round(decimals=1)

    # Filter data based on successful recordings (duration of at least 1 min / filesize of at least 46.1 MB)
    df = df[(df['FileSize'] >= 46.1) | (df['Duration'] >= 60)].reset_index(drop=True)

    # Find the hour at which each recording was made
    def findHour(df):
        try:
            # Convert string timestamps to Datetime objects
            datetimes = pd.to_datetime(df['StartDateTime'], errors='coerce')
            hours = pd.DataFrame(datetimes.dt.hour.fillna(-1))
            hours = hours.astype(int)
            missingEntries = list(hours[hours['StartDateTime'] == -1].index)

            # Use Comment column to find timestamp if not present in StartDateTime
            for idx in missingEntries:
                comment = df.iloc[idx]['Comment']
                time = re.findall(r'\b(\d{2}):(\d{2}):(\d{2})\b', comment)
                hours.iloc[idx]['StartDateTime'] = int(time[0][0])

            # Convert hours to a list for Hour column in df
            return hours['StartDateTime'].tolist()
        except:
            # Dataframe is empty
            return []
    
    # Extract hours of each timestamp
    df['Hour'] = findHour(df)

    # find number of hours covered for each Audiomoth
    grouped = pd.DataFrame(df.groupby('AudioMothCode')['Hour'].nunique()).reset_index()

    # Codes with recordings for all 24 hours
    validCodes = list(grouped[grouped['Hour'] == 24]['AudioMothCode'])

    # No successful Audiomoths found
    if not validCodes:
        return False
    
    # Write data to CSV file
    with open('stratified.csv', 'w') as f:
        f.write('AudioMothCode,AudioMothID,SourceFile,Directory,FileName,FileSize,Encoding,NumChannels,SampleRate,AvgBytesPerSec,BitsPerSample,StartDateTime,Duration,Error,Comment,Artist,FileCreateDate,FileType,FileTypeExtension,MIMEType\n')
        for code in validCodes:
            subset = df[(df['AudioMothCode'] == code)]

            # Use ungrouped keys to perform stratified random sample for each Hour entry
            sampled = subset.groupby(by='Hour', group_keys=False).apply(lambda x: x.sample(n=1))

            # Write entry to new file
            for index, line in sampled.iterrows():
                entry = ','.join([str(line[col]) for col in cols])
                f.write(entry + '\n')

    # Close file for file security
    f.close()

    # CSV file was successfully made
    return True
