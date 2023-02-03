#!/usr/bin/env python
# coding: utf-8
import sys
import pandas as pd
import numpy as np
from pandasql import sqldf
# import xlsxwriter
from sqlalchemy import create_engine
import pymysql

engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root",pw="yash2000",db="optml"))

No_WorkingDays = 250
Hours_perShift = 8
ProductPrice = 10^7

df = pd.read_excel(sys.argv[1],sheet_name= 'Health Facility Master',index_col=None)
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
for col in df.columns:
    if pd.notna(df[col][0]) and type(df[col][0]) == str:
        df[col] = df[col].str.upper()
        
new_cols = ['Name','Address','City','Province','Country','Latitude','Longitude','Admin Area','Facility Level','Sector','HIVCapable','TBCapable','Factor 1','Factor 2','Factor 3','Factor 4','Location Type','Status','Notes']
old_cols = df.columns

for i in range(19):
       df.rename(columns={old_cols[i]:new_cols[i]},inplace = True)

Input_HF = df.drop_duplicates()

df = pd.read_excel(sys.argv[1],sheet_name= 'Labs',index_col=None)
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

for col in df.columns:
    if pd.notna(df[col][0]) and type(df[col][0]) == str:
        df[col] = df[col].str.upper()

old_cols = df.columns
new_cols = ['Name','Address','City','Province','Country','Latitude','Longitude','Admin Area','Facility Level','Sector','HIVCapable','TBCapable','Factor 1','Factor 2','Factor 3','Factor 4','Location Type','Status']


for i in range(18):
       df.rename(columns={old_cols[i]:new_cols[i]},inplace = True)


Input_Lab = df.drop_duplicates()

df = pd.read_excel(sys.argv[1],index_col=None,sheet_name= 'Hubs')
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]


for col in df.columns:
    if pd.notna(df[col][0]) and type(df[col][0]) == str:
        df[col] = df[col].str.upper()

old_cols = df.columns
new_cols = ['Name','Address','City','Province','Country','Latitude','Longitude','Admin Area','Facility Level','Sector','HIVCapable','TBCapable','Factor 1','Factor 2','Factor 3','Factor 4','Location Type','Status']

for i in range(18):
       df.rename(columns={old_cols[i]:new_cols[i]},inplace = True)

Input_Hub = df.drop_duplicates()


df = Input_HF.loc[Input_HF['Status'] == 'INCLUDE']
df = df[['Name','Latitude','Longitude','Address','City','Facility Level','Sector','Admin Area','Province']]

df['Location Type'] = 'HEALTH FACILITY'
df['HIVCapable'] = 'YES'
df['TB TMT Site'] = 'YES'
df['Factor 2'] =''
df['Factor 3'] =''
df['Factor 4'] =''

WIP_HF = df

df = Input_Lab.loc[Input_Lab['Status'] == 'INCLUDE']
df = df[['Name','Latitude','Longitude','Address','City','Facility Level','Sector','Admin Area','Province']]


df['Name'] = df['Name'].replace('SITE_','', regex = True)
df['Name'] = 'LAB_'+df['Name'].astype(str)
df['Location Type'] = 'LAB'
df['HIVCapable'] = 'YES'
df['TB TMT Site'] = 'YES'
df['Factor 2'] =''
df['Factor 3'] =''
df['Factor 4'] =''

WIP_Lab = df.drop_duplicates()

df = Input_Hub.loc[Input_Hub['Status'] == 'INCLUDE']
df = df[['Name','Latitude','Longitude','Address','City','Facility Level','Sector','Admin Area','Province']]

df['Name'] = df['Name'].replace('SITE_','', regex = True)
df['Name'] = 'HUB_'+df['Name'].astype(str)
df['Location Type'] = 'HUB'
df['HIVCapable'] = 'YES'
df['TB TMT Site'] = 'YES'
df['Factor 2'] =''
df['Factor 3'] =''
df['Factor 4'] =''

WIP_Hub = df.drop_duplicates()

Unique_Labs = WIP_Lab['Name'].unique()

df_lis = [WIP_HF , WIP_Lab , WIP_Hub]
WIP_Sites = pd.concat(df_lis)

Output_Sites = WIP_Sites

df = pd.read_excel(sys.argv[1],index_col=None,sheet_name= 'Tests')
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

for col in df.columns:
    if pd.notna(df[col][0]) and type(df[col][0]) == str:
        df[col] = df[col].str.upper()

old_cols = df.columns
new_cols = ['Name','Referral Type','Status','Notes']

for i in range(4):
       df.rename(columns={old_cols[i]:new_cols[i]},inplace = True)

df = df.loc[df['Status'] == 'INCLUDE']

Output_Tests = pd.DataFrame()

Output_Tests['Test Name'] = df['Name']
Output_Tests = Output_Tests.drop_duplicates()

#reading demand data 
df = pd.read_excel(sys.argv[1],index_col=None,sheet_name= "HF Demand")
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]


for col in df.columns:
    if pd.notna(df[col][0]) and type(df[col][0]) == str:
        df[col] = df[col].str.upper()

old_cols = df.columns
new_cols = ['HealthFacility','Test','Demand','Status','Notes']

for i in range(df.columns.size):
       df.rename(columns={old_cols[i]:new_cols[i]},inplace = True)

df['Demand'] = pd.to_numeric(df['Demand'])

df1 = df.query('Status == "INCLUDE"').groupby(['HealthFacility', 'Test', 'Status', 'Notes']).agg({'Demand': 'sum'})

df1  = df1.reset_index()

df1['Quantity'] = df1['Demand'].astype(str)

Output_HFDemand = df1[["HealthFacility","Test","Notes","Quantity"]].drop_duplicates()

df = pd.read_excel(sys.argv[1],index_col=None,sheet_name= "Lab Device Parameters")
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]


for col in df.columns:
    if pd.notna(df[col][0]) and type(df[col][0]) == str:
        df[col] = df[col].str.upper()

df = df.drop_duplicates()

old_cols = df.columns
new_cols = ['Machine','Lab','nDevices','nShifts','Status','Notes']


for i in range(df.columns.size):
       df.rename(columns={old_cols[i]:new_cols[i]},inplace = True)

df['nDevices'] = pd.to_numeric(df['nDevices'])

Input_MachineLab = df

df1 = df.query('Status == "INCLUDE"').groupby(['Lab','Machine','nShifts']).agg({'nDevices': 'sum'})

WIP_LabDev  = df1.reset_index()
WIP_LabDev.head()

nLab = []
nmachine = []
nshift = []
ndev = []
new_machine = []
machine_num = []

for row in range(WIP_LabDev.shape[0]):
    lab = WIP_LabDev.iloc[row]["Lab"]
    machine = WIP_LabDev.iloc[row]["Machine"]
    nDevices = int(WIP_LabDev.iloc[row]["nDevices"])
    shift = WIP_LabDev.iloc[row]["nShifts"]
    
    for dev in range(1, nDevices+1):
        nLab.append(lab)
        nmachine.append(machine)
        nshift.append(shift)
        ndev.append(nDevices)
        new_machine.append(machine + '_' + str(shift) + ' Shift' + '_' + str(dev))
        machine_num.append(machine + '_' + str(dev))


Output_MachineLab = pd.DataFrame({'Lab':nLab, 'Machine':nmachine, 'Maximum # of Shifts':nshift, '# of Existing Devices':ndev,'New_Device':new_machine,'Machine_Num':machine_num})

Output_MachineLab['Lab_New'] = 'LAB_'+Output_MachineLab['Lab'].astype(str)

WIP_MachineLabV1 = Output_MachineLab.copy()
WIP_LabDev1 = Output_MachineLab.copy()

NewOutput_LabDevice = Output_MachineLab[['Lab_New' ,'New_Device']].drop_duplicates()
NewOutput_LabDevice = NewOutput_LabDevice.rename(columns={'Lab_New':'Lab','New_Device':'Device'})


#Error checking in sites table
Output_SitesV1 = Output_Sites.copy()
Val_NullSites = pd.DataFrame(columns=['Data' , 'Warning'])
df1= Output_SitesV1[Output_SitesV1['Name'].isna()].drop_duplicates()
Val_NullSites['Data'] = df1['Admin Area']+"+"+df1['Sector']+"+"+df1['Facility Level']
Val_NullSites['Warning'] = 'Some Labs do not have devices in given admin areas'


#Duplicate checking in sites table
Val_DuplicateSites = pd.DataFrame(columns=['Data' , 'Warning'])
Val_DuplicateSites['Data'] = Output_SitesV1[Output_SitesV1.duplicated()].drop_duplicates()['Name']
Val_DuplicateSites['Warning'] = 'Duplicate sites in model input'

df = pd.read_excel(sys.argv[1],index_col=None,sheet_name= "Device Test Parameters")
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

df = df.drop(columns=['Z_DeviceTest'] ,axis = 1)

for col in df.columns:
    if pd.notna(df[col][0]) and type(df[col][0]) == str:
        df[col] = df[col].str.upper()
        
old_cols = df.columns
new_cols = ['Machine Type','Test Type','Capacity','Cost of Test','Status','Notes']

for i in range(df.columns.size):
       df.rename(columns={old_cols[i]:new_cols[i]},inplace = True)

WIP_LabMachineTest = df.loc[df['Status'] == 'INCLUDE'].drop_duplicates()

Output_LabMachineTest = WIP_LabMachineTest.copy()

Output_LabMachineTest = Output_LabMachineTest.drop(columns=['Status'], axis = 1)

df = Output_LabMachineTest[Output_LabMachineTest['Cost of Test'].notna()]
df = Output_LabMachineTest[Output_LabMachineTest['Capacity'].notna()]

Map_Dev_NewDev = Output_MachineLab[['Machine' , 'New_Device']].drop_duplicates()

df3 = pd.merge(Output_LabMachineTest,Map_Dev_NewDev,left_on = 'Machine Type',right_on = 'Machine', how = 'left')
df3 = df3[['New_Device','Test Type','Capacity','Cost of Test','Notes']].drop_duplicates()
df3 = df3.rename(columns={'New_Device':'Device'})

NewOutput_DeviceTest = df3.copy()

df = pd.read_excel(sys.argv[1],index_col=None,sheet_name= "Devices")
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

for col in df.columns:
    if pd.notna(df[col][0]) and type(df[col][0]) == str:
        df[col] = df[col].str.upper()


df = df.drop(columns = ['Number of Modules'] , axis = 1 )

old_cols = df.columns
new_cols = ['Device','Shift Cost','Overhead Cost','Startup Cost','Available hours','Status','Notes']

for i in range(df.columns.size):
       df.rename(columns={old_cols[i]:new_cols[i]},inplace = True)

WIP_Machine = df.loc[df['Status'] == 'INCLUDE']

Output_Machine = WIP_Machine.copy()
Output_Machine.drop(columns =['Status', 'Notes'],inplace = True)

df3 = pd.merge(Output_Machine,Map_Dev_NewDev,left_on="Device",right_on ="Machine", how = 'left')
df3 = df3[['New_Device','Shift Cost','Overhead Cost','Startup Cost' , 'Available hours']]

df3 = df3.rename(columns = {'New_Device':'Device'})
NewOutput_Machine = df3.dropna(how='all')

WIP_Machine['StepCost'] = WIP_Machine['Device']+"_"+"FOC"

WIP_MachineStepCost = WIP_Machine[['Device','StepCost','Available hours','Shift Cost', 'Overhead Cost']]

WIP_MachineStepCost['MaxHours']=WIP_MachineStepCost['Available hours']*No_WorkingDays

WIP_MachineStepCost.head()

POC_Machine = (Output_Machine.loc[ (Output_Machine['Device']=='^GENEXPERT') & (Output_Machine['Device'] != "GENEXPERT INFINITY-48")])['Device'].drop_duplicates()

POC_Machine = list(POC_Machine)

POC_Machine.append(' mPIMA')

df = pd.read_excel(sys.argv[1],index_col=None,sheet_name= "Historical Referrals")
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

for col in df.columns:
    if df.shape[0] == 0:
        break
    if pd.notna(df[col][0]) and type(df[col][0]) == str:
        df[col] = df[col].str.upper()

old_cols = df.columns
new_cols = ['Origin','Origin Type' ,'Destination','Dest Type','Test','Mode','Annual Samples referred','Type','Status','Notes']


for i in range(df.columns.size):
       df.rename(columns={old_cols[i]:new_cols[i]},inplace = True)


Input_HistoricalReferrals = df.copy()

WIP_HistoricalReferrals = pd.DataFrame(columns=Input_HistoricalReferrals.columns) 
WIP_HistoricalReferrals = WIP_HistoricalReferrals.assign(Origin2 = lambda x : [x['Origin'] if x['Origin Type']=='HF' else 'HUB_'+x['Origin'] if x['Origin Type']=='HUB' else 'LAB_'+x['Origin'] for x in WIP_HistoricalReferrals.to_dict('records')])     

WIP_HistoricalReferrals = WIP_HistoricalReferrals.assign(Dest2 = lambda x: [x['Destination'] if x['Dest Type'] == 'HF' else 'HUB_'+ x['Destination'] if x['Dest Type'] == 'HUB' else 'LAB_'+ x['Destination'] for x in WIP_HistoricalReferrals.to_dict('records')])

NewOutput_HistoricalReferrals = WIP_HistoricalReferrals.groupby(['Origin2','Origin Type','Dest2','Dest Type' , 'Test']).agg({'Annual Samples referred': 'sum'})

NewOutput_HistoricalReferrals = NewOutput_HistoricalReferrals.rename(columns = {'Annual Samples referred':'Total Samples referred'})

NewOutput_HistoricalReferrals = NewOutput_HistoricalReferrals.reset_index()

NewOutput_LabDeviceTest = pd.merge(NewOutput_LabDevice, NewOutput_DeviceTest, on = 'Device', how='left')


df = pd.read_excel(sys.argv[1],index_col=None,sheet_name= "Historical Testing")
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]


for col in df.columns:
    if df.shape[0] == 0:
        break
    if pd.notna(df[col][0]) and type(df[col][0]) == str:
        df[col] = df[col].str.upper()

df = df.drop(columns = ['LabDevice','DataConsistency'] , axis = 1)


old_cols = df.columns
new_cols = ['Lab','Device' ,'Test','Status','Total Samples tested','Type']

for i in range(df.columns.size):
       df.rename(columns={old_cols[i]:new_cols[i]},inplace = True)

Input_HistoricalTesting = df.copy()

WIP_Input_HistoricalTesting1 = Input_HistoricalTesting.groupby(['Lab', 'Device', 'Test'])['Total Samples tested'].sum().reset_index()

WIP_Input_HistoricalTesting1['Lab'] = 'LAB_'+WIP_Input_HistoricalTesting1['Lab']

WIP_Input_HistoricalTesting1['Total Samples tested']= WIP_Input_HistoricalTesting1['Total Samples tested'].astype(int)
WIP_Input_HistoricalTesting1=WIP_Input_HistoricalTesting1.reset_index(drop=True)



WIP_Input_HistoricalTesting1['Lab'] = WIP_Input_HistoricalTesting1['Lab'].astype(str)
WIP_Input_HistoricalTesting1['Device'] = WIP_Input_HistoricalTesting1['Device'].astype(str)


WIP_LabDev1['Lab_New'] =WIP_LabDev1['Lab_New'].astype(str)
WIP_LabDev1['Machine'] =WIP_LabDev1['Machine'].astype(str)


WIP_Input_HistoricalTesting1 = WIP_Input_HistoricalTesting1.merge(WIP_LabDev1, left_on=['Lab','Device'], right_on=['Lab_New','Machine'], how='left')
WIP_Input_HistoricalTesting1 = WIP_Input_HistoricalTesting1.assign(Historical_Testing = WIP_Input_HistoricalTesting1['Total Samples tested'] / WIP_Input_HistoricalTesting1['# of Existing Devices'])
WIP_Input_HistoricalTesting1 = WIP_Input_HistoricalTesting1.groupby(['Lab_x','New_Device','Test'])['Historical_Testing'].sum().reset_index()


WIP_Input_HistoricalTesting1.rename(columns = {'Lab_x':'Lab','New_Device':'Device'},inplace=True)

y = NewOutput_LabDeviceTest.merge(WIP_Input_HistoricalTesting1,left_on =['Lab','Device','Test Type'],right_on=['Lab','Device','Test'],how = 'left')
y['DeviceType'] = [x.split('_')[0] for x in y['Device']]

New_OutputHistoricalTesting = y.merge(NewOutput_Machine,on = "Device",how = 'left')
New_OutputHistoricalTesting['Workingdays'] = No_WorkingDays
New_OutputHistoricalTesting['Hours_Consumed'] = (New_OutputHistoricalTesting['Available hours']/New_OutputHistoricalTesting['Capacity'])*New_OutputHistoricalTesting['Historical_Testing']
New_OutputHistoricalTesting['Annual Hours'] = New_OutputHistoricalTesting['Available hours']*New_OutputHistoricalTesting['Workingdays']


New_OutputHistoricalTestingPivot = New_OutputHistoricalTesting.groupby(['Lab','Device','DeviceType']).agg({'Historical_Testing':'sum','Hours_Consumed':'sum', 'Annual Hours':'mean'})
New_OutputHistoricalTestingPivot['Capacity_Utilization'] = New_OutputHistoricalTestingPivot['Hours_Consumed']/New_OutputHistoricalTestingPivot['Annual Hours']
New_OutputHistoricalTestingPivot.rename(columns = {'Historical_Testing':'Hist_test','Hours_Consumed':'Hours_Consumed1','Annual Hours':'Annual_Hours1'},inplace = True)

New_OutputHistoricalTestingPivot = New_OutputHistoricalTestingPivot.reset_index()


temp = New_OutputHistoricalTestingPivot.copy()
temp['Lab'] = [x.split('_')[1] for x in temp['Lab']]
mergeCapacity = temp.merge(Output_SitesV1,left_on = 'Lab' , right_on = 'Name' , how = "left")

# df_list= [Output_SitesV1,Output_Tests,Output_HFDemand,NewOutput_Machine,NewOutput_LabDeviceTest,New_OutputHistoricalTesting,New_OutputHistoricalTestingPivot,mergeCapacity]
# sheets = ['Locations','Tests','HF Demand','Device Info','Lab Device Test','Historical Testing', 'Historical Testing Pivot','Merge Capacity']

# Excelwriter = pd.ExcelWriter(f'uploads/merge_{sys.argv[2]}',engine="xlsxwriter")
# for i in range(len(df_list)):
#     df_list[i].to_excel(Excelwriter,sheet_name = sheets[i],index = False)
# Excelwriter.save()

mergeCapacity.to_sql(con=engine, name= 'merge_cap_'+sys.argv[2].split('.')[0], if_exists='replace',index = False)

print('runned successfully...')