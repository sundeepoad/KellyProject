import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import math

# Read your CSV files
df = pd.read_csv("CTA Attendance.csv")
df1 = pd.read_csv("RTA (Daily Attendance).csv")
df2 = pd.read_csv("Logged in and Logged out states with date and time - AP Interpreters.csv")

#df2 = df2[df2['agentState'] == 'LoggedOut']

#df1.rename(columns = {'INT #', 'INT#'}, inplace = True)

df1 = df1[df1['Location Name'].str.contains('KSUSA|VRI Mentor', case=False, na=False)]
df1['Location Name'] = df1['Location Name'].str.replace('KSUSA -', '', regex=False)

df['Display Name'] = df['Display Name'].str.replace(r'\*[\d]+', '', regex=True)
df1['Display Name'] = df1['Display Name'].str.replace(r'\*[\d]+', '', regex=True)

# Rename and clean other columns
df.rename(columns={'Area Name': 'Location Name'}, inplace=True)
df["INT#"] = df["Last Name"].str.extract('(\d+)')
df1["INT#"] = df["Last Name"].str.extract('(\d+)')

# Merge dataframes
merged_df = pd.merge(df, df1, on=['INT#','Display Name','Location Name', 'Schedule Start Time','Schedule End Time', 'Timesheet Start Time','Schedule Total Time', 'Schedule Date'], how='outer')

# Clean merged dataframe
df2.rename(columns={'sfId':'INT#', 'name':'naam'}, inplace=True)
merged_df = merged_df.dropna(subset=['Display Name', 'Timesheet Start Time'])

# Convert columns to proper formats
merged_df["INT#"] = pd.to_numeric(merged_df["INT#"], errors='coerce')
merged_df = merged_df.dropna(subset=["INT#"])


final = pd.merge(merged_df, df2, on="INT#", how='inner')


# Ensure correct time formats and handle missing data
final['startDate'] = pd.to_datetime(final['startDate'], errors='coerce').dt.time  # Convert 'startDate' and handle errors
final['Schedule Start Time'] = pd.to_datetime(final['Schedule Start Time'].str[:5], format='%H:%M', errors='coerce').dt.time  # Convert 'Schedule Start Time'
final['Schedule End Time'] = pd.to_datetime(final['Schedule End Time'].str[:5], format='%H:%M', errors='coerce').dt.time  # Convert 'Schedule Start Time'

################################

# Remove records where 'startDate' is before 'Schedule Start Time'
def filter_valid_records(row):
    # Ensure valid times (both startDate and Schedule Start Time must not be NaT)
    if pd.isna(row['Schedule End Time']) or pd.isna(row['startDate']):
        return False
    
    # Convert times to datetime objects for comparison (ignore the date part)
    time1 = pd.to_datetime(str(row['Schedule End Time']), format='%H:%M:%S')
    time2 = pd.to_datetime(str(row['startDate']), format='%H:%M:%S')   ### logout time
    
    
    return time2 < time1


valid_final = final

valid_final['Logged Out Before'] = valid_final.apply(filter_valid_records, axis=1)

# Filter out invalid records where startDate is earlier than Schedule Start Time
#valid_final = final[final.apply(filter_valid_records, axis=1)]

# Function to calculate the time difference in minutes
def time_diff(row):
    # Check if the values are valid times
    if pd.isna(row['Schedule End Time']) or pd.isna(row['startDate']):
        return None  # Return None if the time is invalid (NaT)

    time1 = pd.to_datetime(str(row['Schedule End Time']), format='%H:%M:%S')
    time2 = pd.to_datetime(str(row['startDate']), format='%H:%M:%S')
    time_diff = (time2 - time1).total_seconds() / 60
    #return round(int(time_diff), 0)
    return (time_diff - 1)

valid_final['Time Difference (minutes)'] = valid_final.apply(time_diff, axis=1)

# Add Status column based on lateness criteria (5 minutes or more)
def late_status(row):
    if pd.isna(row['Time Difference (minutes)']):
        return "On Time"  # If the time is NaT or invalid, assume the person was on time
    
    if row['Time Difference (minutes)'] > 0 and row['Logged Out Before'] == False:
        return "Later"  # If the person is late by 5 minutes or more, mark as "Late"


    if row['Time Difference (minutes)'] > -4  and row['Logged Out Before'] == True:
        return "On Time"  # If the person is late by 5 minutes or more, mark as "Late"
    
    
    if row['Time Difference (minutes)'] < -4 and row['Logged Out Before'] == True:
        return "Before"
    
    else:
        return "On Time"  # If within the acceptable window, mark as "On Time"

valid_final['Status'] = valid_final.apply(late_status, axis=1)


valid_final['Schedule Start Time'] = pd.to_datetime(valid_final['Schedule Start Time'], format='%H:%M:%S').dt.strftime('%H:%M')
valid_final['Schedule End Time'] = pd.to_datetime(valid_final['Schedule End Time'], format='%H:%M:%S').dt.strftime('%H:%M')
valid_final['startDate'] = pd.to_datetime(valid_final['startDate'], format='%H:%M:%S').dt.strftime('%H:%M')


####################################

### If there exists 1 record and it is either late or before, remove that.
def filter_records(df):
    # 1. Remove records where there's only 1 record for INT# and it's "Before" or "On Time"
    single_ints = df.groupby('INT#').size() == 1  # Identify INT#s with only 1 record
    remove_single = df[df['INT#'].isin(single_ints[single_ints].index) & df['Status'].isin(['Later', 'On Time'])]

    df = df[~df.index.isin(remove_single.index)]

    return df


###################################
###   LATE LOGIC FUNCTIONS
###################################
### Group by int and schedule start time, if there is either On time or before events, remove those records.
def remove_on_time_or_before(df):

    groups = df.groupby(['INT#', 'Schedule End Time'])


    rows_to_remove = []
    
    # Loop through each group (INT# and Schedule Start Time)
    for _, group in groups:
        # Check if any record in the group has 'On Time' or 'Before' in Status
        condition = group['Status'].isin(['On Time', 'Later']) & (group['Time Difference (minutes)'].abs() <= 60)
        
        # If the condition is met, add all rows in this group to the rows_to_remove list
        if condition.any():
            rows_to_remove.extend(group.index.tolist())
    
    # Remove the identified rows from the DataFrame
    df = df.drop(rows_to_remove)
    
    return df


### If all records against some INT# and Schedule Start time are late, show late with Minimum Time Difference

def all_late(df):
    mygroup = df.groupby(["INT#", 'Schedule End Time'])
    keep = []
    
    for _, g in mygroup:
        # Check if there are any 'Late Login' statuses in the group
        late_logins = g[g['Status'] == 'Before']
        
        # If there are late logins, get the one with the minimum Time Difference
        if not late_logins.empty:
            min_time_row = late_logins.loc[late_logins['Time Difference (minutes)'].idxmax()]
            keep.append(min_time_row.name)  # Keep only the index of the row with minimum time difference
    
    # Filter the DataFrame to keep only the rows with the indices in 'keep'
    df = df.loc[keep]
    
    return df

###################################

valid_final = filter_records(valid_final)
valid_final = remove_on_time_or_before(valid_final)
valid_final = all_late(valid_final)


#########################################################################################################

#### function to see if agent state = loggedin == Not Logged Out
###  if event : logged out = Early Log out

def early_or_not(row):

    if row["agentState"] == "LoggedIn":
        return "Not Logged Out"

    else:
        return "Early Logout"


valid_final['Final Status'] = valid_final.apply(early_or_not, axis=1)
valid_final['Time Difference (minutes)'] = valid_final['Time Difference (minutes)'].apply(np.ceil)



######################################################################
valid_final = valid_final.sort_values(by = 'Display Name', ascending = True)
valid_final = valid_final[['INT#','Display Name', 'Location Name', 'Schedule Start Time','Schedule End Time','agentState', 'startDate', 'Time Difference (minutes)', 'Status',
                           'Logged Out Before','Final Status']]
valid_final.to_excel("early 1.xlsx", header=True, index=False)
print("RUN SUCCESSFUL")


