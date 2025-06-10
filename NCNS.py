import pandas as pd

## ONLY WORKS FOR NCNS
### This sheet cleans data frames from US & Canada Deputy and merges those. Also extract only rows where
### Timesheet Start time is Empty 


df = pd.read_csv("CTA Attendance.csv")
df1 = pd.read_csv("RTA (Daily Attendance).csv")

#df1.rename(columns={'INT #': 'INT#'}, inplace=True)


#df1.head(5)
# Filter and clean df1
df1 = df1[df1['Location Name'].str.contains('KSUSA|VRI Mentor', case=False, na=False)]
df1['Location Name'] = df1['Location Name'].str.replace('KSUSA -', '', regex=False)

# Removes number and keeps only the Text
df['Display Name'] = df['Display Name'].str.replace(r'\*[\d]+', '', regex=True)
df1['Display Name'] = df1['Display Name'].str.replace(r'\*[\d]+', '', regex=True)

# Removing Last text from last name and keeps only number
df["INT#"] = df["Last Name"].str.extract('(\d+)')
df1["INT#"] = df1["Last Name"].str.extract('(\d+)')

# Matching naming convention as per Canadian deputy results
df.rename(columns={'Area Name': 'Location Name'}, inplace=True)

# Drop Last Name column (this is done only once)
df.drop(columns=['Last Name'], inplace=True)

# Merge df and df1

df['INT#'] = df['INT#'].astype(str)
df1['INT#'] = df1['INT#'].astype(str)

merged_df = pd.merge(df, df1, on=['INT#','Display Name','Location Name', 'Schedule Start Time', 'Timesheet Start Time', 'Schedule Total Time', 'Schedule Date','Schedule Open', 'Email'], how='outer')

# Remove null records from Display Name and filter rows where Timesheet Start Time is empty
merged_df = merged_df.dropna(subset=['Display Name'])
merged_df = merged_df[merged_df['Timesheet Start Time'].isna()]

# Convert Schedule Total Time from hours to minutes
merged_df['Schedule Total Time'] = merged_df['Schedule Total Time'] * 60



# Load the detailed.csv file
detailed_df = pd.read_csv("Logged in and Logged out states with date and time - AP Interpreters.csv")



# Convert INT# to string in merged_df and sfId to string in detailed_df
merged_df['INT#'] = merged_df['INT#'].astype(str)
detailed_df['sfId'] = detailed_df['sfId'].astype(str)

# Merge detailed_df with merged_df based on INT# (sfId in detailed_df)
final_df = pd.merge(merged_df, detailed_df, left_on='INT#', right_on='sfId', how='outer')


final_df.drop(columns=['bu','name','sfId'], inplace=True)

final_df['Blank 1'] = ''
final_df['Blank 2'] = ''

ordered_columns = ['INT#', 'Display Name','Blank 1','Blank 2','Schedule Start Time', 'Schedule Total Time', 'Location Name', 'Schedule Date','Schedule Open','Email','startDate','agentState']

final_df = final_df[ordered_columns]
# Write the final output to an Excel file
final_df.to_excel("NCNS_Final.xlsx", header=True, index=False)

print("****** NCNS FILE HAS BEEN CREATED ********")
