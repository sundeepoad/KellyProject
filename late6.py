## logout function is not comparing case where logout is same time as login. Someone logout at 12:12:04 and login at 12:12:08. It doesnot compare these cases properly.
## reason is, startDate and LoggedIm don't keep track of seconds in above functions. 

import pandas as pd
from datetime import datetime, timedelta
import math
import numpy as np

# Read your CSV files
df = pd.read_csv("CTA Attendance.csv")
df1 = pd.read_csv("RTA (Daily Attendance).csv")
df2 = pd.read_csv("Logged in and Logged out states with date and time - AP Interpreters.csv")
df3 = pd.read_csv("untitled.csv")

df2.rename(columns={'sfId':'INT#', 'name':'naam'}, inplace=True)
# Clean the data
df1.rename(columns={'INT #': 'INT#'}, inplace=True)

df1 = df1[df1['Location Name'].str.contains('KSUSA|VRI Mentor', case=False, na=False)]
df1['Location Name'] = df1['Location Name'].str.replace('KSUSA -', '', regex=False)


logout = df2[df2['agentState'] == 'LoggedOut']
print("Logouts: ",logout)

df2 = df2[df2['agentState'] == 'LoggedIn']

df['Display Name'] = df['Display Name'].str.replace(r'\*[\d]+', '', regex=True)
df1['Display Name'] = df1['Display Name'].str.replace(r'\*[\d]+', '', regex=True)

# Rename and clean other columns
df.rename(columns={'Area Name': 'Location Name'}, inplace=True)
df["INT#"] = df["Last Name"].str.extract('(\d+)')
df1["INT#"] = df1["Last Name"].str.extract('(\d+)')

# Merge dataframes

df['INT#'] = df['INT#'].astype(str)
df1['INT#'] = df1['INT#'].astype(str)
merged_df = pd.merge(df, df1, on=['INT#','Display Name','Location Name', 'Schedule Start Time', 'Timesheet Start Time','Schedule Total Time', 'Schedule Date'], how='outer')

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




# Remove records where 'startDate' is before 'Schedule Start Time'
def filter_valid_records(row):
    # Ensure valid times (both startDate and Schedule Start Time must not be NaT)
    if pd.isna(row['Schedule Start Time']) or pd.isna(row['startDate']):
        return False
    
    # Convert times to datetime objects for comparison (ignore the date part)
    time1 = pd.to_datetime(str(row['Schedule Start Time']), format='%H:%M:%S')
    time2 = pd.to_datetime(str(row['startDate']), format='%H:%M:%S')
    
    # Return False if 'startDate' is earlier than 'Schedule Start Time'
    return time2 <= time1


valid_final = final

valid_final['Logged Before'] = valid_final.apply(filter_valid_records, axis=1)

# Filter out invalid records where startDate is earlier than Schedule Start Time
#valid_final = final[final.apply(filter_valid_records, axis=1)]

# Function to calculate the time difference in minutes
def time_diff(row):
    # Check if the values are valid times
    if pd.isna(row['Schedule Start Time']) or pd.isna(row['startDate']):
        return None  # Return None if the time is invalid (NaT)

    time1 = pd.to_datetime(str(row['Schedule Start Time']), format='%H:%M:%S')
    time2 = pd.to_datetime(str(row['startDate']), format='%H:%M:%S')
    time_diff = (time2 - time1).total_seconds() / 60
    return round(int(time_diff), 0)

valid_final['Time Difference (minutes)'] = valid_final.apply(time_diff, axis=1)

# Add Status column based on lateness criteria (5 minutes or more)
def late_status(row):
    if pd.isna(row['Time Difference (minutes)']):
        return "On Time"  # If the time is NaT or invalid, assume the person was on time
    
    if row['Time Difference (minutes)'] >= 5:
        return "Late Login"  # If the person is late by 5 minutes or more, mark as "Late"
    
    if row['Logged Before'] == True:
        return "Before"
    
    else:
        return "On Time"  # If within the acceptable window, mark as "On Time"

valid_final['Status'] = valid_final.apply(late_status, axis=1)


valid_final['Schedule Start Time'] = pd.to_datetime(valid_final['Schedule Start Time'], format='%H:%M:%S').dt.strftime('%H:%M')
valid_final['startDate'] = pd.to_datetime(valid_final['startDate'], format='%H:%M:%S').dt.strftime('%H:%M')


####################################

### If there exists 1 record and it is either late or before, remove that.
def filter_records(df):
    # 1. Remove records where there's only 1 record for INT# and it's "Before" or "On Time"
    single_ints = df.groupby('INT#').size() == 1  # Identify INT#s with only 1 record
    remove_single = df[df['INT#'].isin(single_ints[single_ints].index) & df['Status'].isin(['Before', 'On Time'])]

    df = df[~df.index.isin(remove_single.index)]

    return df


###################################
###   LATE LOGIC FUNCTIONS
###################################
### Group by int and schedule start time, if there is either On time or before events, remove those records.
def remove_on_time_or_before(df):

    groups = df.groupby(['INT#', 'Schedule Start Time'])


    rows_to_remove = []
    
    # Loop through each group (INT# and Schedule Start Time)
    for _, group in groups:
        # Check if any record in the group has 'On Time' or 'Before' in Status
        condition = group['Status'].isin(['On Time', 'Before']) & (group['Time Difference (minutes)'].abs() <= 60)
        
        # If the condition is met, add all rows in this group to the rows_to_remove list
        if condition.any():
            rows_to_remove.extend(group.index.tolist())
    
    # Remove the identified rows from the DataFrame
    df = df.drop(rows_to_remove)
    
    return df


### If all records against some INT# and Schedule Start time are late, show late with Minimum Time Difference

def all_late(df):
    mygroup = df.groupby(["INT#", "Schedule Start Time"])
    keep = []
    
    for _, g in mygroup:
        # Check if there are any 'Late Login' statuses in the group
        late_logins = g[g['Status'] == 'Late Login']
        
        # If there are late logins, get the one with the minimum Time Difference
        if not late_logins.empty:
            min_time_row = late_logins.loc[late_logins['Time Difference (minutes)'].idxmin()]
            keep.append(min_time_row.name)  # Keep only the index of the row with minimum time difference
    
    # Filter the DataFrame to keep only the rows with the indices in 'keep'
    df = df.loc[keep]
    
    return df

###################################

valid_final = filter_records(valid_final)
valid_final = remove_on_time_or_before(valid_final)
valid_final = all_late(valid_final)



valid_final = valid_final.sort_values(by = 'Display Name', ascending = True)

valid_final['New Col'] = 'LoggedIn'

# Reorder the columns as specified 
ordered_columns = ['INT#', 'Display Name', 'New Col', 'startDate', 'Schedule Start Time', 'Time Difference (minutes)', 'Location Name', 'Status']

# Create a new dataframe with the columns in the specific order
valid_final = valid_final[ordered_columns]

##############################  latest lates work with first login
####
df3.rename(columns={'ID': 'INT#'}, inplace=True)

valid_final2 = pd.merge(valid_final, df3, on=['INT#'], how='outer')

# Ensure First Login is datetime after merge
valid_final2['First Login'] = pd.to_datetime(valid_final2['First Login'], errors='coerce')
# Format time as HH:MM
valid_final2['First Login'] = valid_final2['First Login'].dt.strftime('%H:%M')



# Compare entire datetime values
valid_final2['Login Check'] = np.where(
    valid_final2['startDate'] == valid_final2['First Login'],
    "DON'T CHECK",
    "CHECK"
)





########################
# Convert times to datetime.time for comparison
logout['startDate'] = pd.to_datetime(logout['startDate'], errors='coerce')
logout['logout_time'] = logout['startDate'].dt.time

print("logout lougout: ",logout['logout_time'])

 #Function to check if a logout occurred between schedule and actual login
def check_logout_between(row):
    # Filter logout entries for the same INT#
    relevant_logouts = logout[logout['INT#'] == row['INT#']]

    if pd.isna(row['Schedule Start Time']) or pd.isna(row['startDate']):
        return "LATE"

    # Convert strings to datetime.time objects (with or without seconds)
    schedule_time = datetime.strptime(row['Schedule Start Time'], "%H:%M").time()
    login_time = datetime.strptime(row['startDate'], "%H:%M").time()
   # logout_time = datetime.strptime(row['logout_time'], "%H:%M").time()
    for _, lo in relevant_logouts.iterrows():
        logout_time = lo['logout_time']  # already datetime.time

        if schedule_time < logout_time < login_time:
            return "NOT LATE"

        if logout_time == login_time and schedule_time < login_time:
            return "NOT LATE: Same logout as Login"

    return "LATE"





# Apply the function to the final DataFrame
valid_final2['Skip'] = valid_final2.apply(check_logout_between, axis=1)


#valid_final = valid_final[['INT#','Display Name', 'Location Name', 'Schedule Start Time', 'startDate', 'Time Difference (minutes)', 'Status']]
valid_final2.to_excel("lates6.xlsx", header=True, index=False)
print("RUN SUCCESSFUL")


