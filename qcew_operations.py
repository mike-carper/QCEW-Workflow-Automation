import pandas as pd
import numpy as np
import geopandas as gpd

own_codes = {1:'public', 2:'public', 3:'public', 5:'private'}
cpi_dict_us = {2000:172.2, 2001:177.1, 2002:179.9, 2003:184.0, 2004:188.9, 2005:195.3, 2006:201.6, 2007:207.342, 2008:215.303,
               2009:214.537, 2010:218.056, 2011:224.939, 2012:229.594, 2013:232.957, 2014:236.736, 2015:237.017, 2016:240.007,
               2017:245.12, 2018:251.107, 2019:255.657, 2020:258.811, 2021:270.97, 2022:292.655}
macro_dict_num = {'11':'Industrial','21':'Industrial','22':'Industrial','23':'Industrial',
                  '31':'Industrial','32':'Industrial','33':'Industrial','42':'Industrial',
                  '44':'Local Services','45':'Local Services','48':'Industrial','49':'Industrial',
                  '51':'Office','52':'Office','53':'Office','54':'Office','55':'Office','56':'Office',
                  '61':'Institutional','62':'Institutional','71':'Local Services','72':'Local Services',
                  '81':'Local Services','99':'Local Services','92':'Government'}
# Load master NAICS crosswalk file
xw_ALL = pd.read_excel('../crosswalks/master_NAICS_Crosswalk_02_07_12_17_22.xlsx')
xw_ALL = xw_ALL.astype(str)

# _______________________________________________________________________________________________________________________________________ #

### OPERATION FUNCTIONS ###

def clean_data(df=None, ownership='private', industry_focus='all', time_frame=range(2000,2023), quarters=['Q1','Q2','Q3','Q4'],
               function='table'):
    # Remove rows without NAICS codes
    df = df[~df['ECONOMIC_SECTOR'].isna()].copy()
    # Clean NAICS code formatting
    df['NAICS'] = df['NAICS'].astype(int).astype(str)
    # Add 2, 4, 6-digit NAICS columns
    df['NAICS_2'], df['NAICS_3'], df['NAICS_4'] = df['NAICS'].str[:2], df['NAICS'].str[:3], df['NAICS'].str[:4]
    df['NAICS_5'], df['NAICS_6'] = df['NAICS'].str[:5], df['NAICS'].str[:6]
    # Create public and private designations
    df['Ownership'] = df['OWN'].map(own_codes)
    # Create macro sectors
    df['MACRO_SECTOR'] = df['NAICS_2'].map(macro_dict_num)
    # Only keep necessary columns
    if function == 'table':
        df_ = df[['Yr','Qtr','UID','OWN','Ownership','MEEI','MACRO_SECTOR','ECONOMIC_SECTOR','SUBSECTOR','INDUSTRY_GROUP',
                  'NAICS','NAICS_2','NAICS_3','NAICS_4','NAICS_5','NAICS_6','AVGEMP','TOT_WAGES','LON','LAT']]
    elif function == 'records':
        df_ = df.copy()
    # Remove HQs to prevent double counts, focus on public, private or both ownerships
    if ownership == 'all':
        dff = df_[df_['MEEI']!=2]
    else:
        dff = df_[(df_['MEEI']!=2)&(df_['Ownership']==ownership)]
    # Focus on industry
    if type(industry_focus) == str:
        if industry_focus == 'all':
            pass
        else:
            dff = dff[dff['ECONOMIC_SECTOR']==industry_focus].copy()
    elif type(industry_focus) == list:
        dff = dff[dff['ECONOMIC_SECTOR'].isin(industry_focus)].copy()  
    # Focus on timeframe
    df_yrs = dff[dff['Yr'].astype(int).isin(time_frame)]            
    # Focus on quarters of interest
    df_qs = df_yrs[df_yrs['Qtr'].isin(quarters)]
    
    return df_qs
    
def assign_ind(industry_level):
    # industry-level variable(s) for groupby
    if industry_level == '2 digit':
        industry = ['ECONOMIC_SECTOR']
    elif industry_level == '3 digit':
        industry = ['ECONOMIC_SECTOR','SUBSECTOR']
    elif industry_level == '4 digit':
        industry = ['ECONOMIC_SECTOR','SUBSECTOR','INDUSTRY_GROUP']
    elif industry_level == 'macro sector':
        industry = ['MACRO_SECTOR']
    elif industry_level == 'TOTAL':
        industry = ['Ownership']
    elif industry_level == 'PDR1':
        industry = ['tier1']
    elif industry_level == 'PDR2':
        industry = ['tier1','tier2']
    elif industry_level == 'PDR3':
        industry = ['tier1','tier2','tier3']

    return industry

def assign_targ(target_var):
    # output variable (establishment, employment or wages)
    if target_var == 'employment':
        target = 'IND_EMPL'
    elif target_var == 'wages':
        target = 'IND_WAGES'
    elif target_var == 'establishments':
        target = 'IND_EST'
    
    return target

def assign_freq_cols(freq):
    # frequency columns (quarterly vs. annual)
    if freq == 'annual':
        freq_cols = ['Yr']
    elif freq == 'quarterly':
        freq_cols = ['Yr','Qtr']
        
    return freq_cols
    
def spatial_join(df=None, shapefile=None):
    time_frame = list(df['Yr'].astype(int).unique())
    # create master GeoDataFrame
    geo_df = gpd.GeoDataFrame()
    # set input shapefile CRS to lat./long.
    shapefile = shapefile.to_crs(4326)
    # yearly/append method to save memory
    for year in time_frame:
        df_yr = df[df['Yr'].astype(int)==year]
        # create GDF of lat./long. points for each establishment
        points = gpd.GeoDataFrame(df_yr, geometry = gpd.points_from_xy(df_yr.LON, df_yr.LAT))
        # join
        gdf_yr = points.sjoin(shapefile, how="inner")
        # append to master GDF
        geo_df = geo_df.append(gdf_yr)
        
    return geo_df
    
def shapeless_geo_filter(df=None, geo_level='cd', geo=None):
    if geo_level == 'tract':
        geo_df = df[df['CENSUS_TRACT_2020'].isin(geo)]
    elif geo_level == 'nta':
        geo_df = df[df['NTA_20'].isin(geo)]
    elif geo_level == 'cd':
        geo_df = df[df['CD'].isin(geo)]
    elif geo_level == 'zipcode':
        geo_df = df[df['UIZIP'].isin(geo)]
    elif geo_level == 'BBL':
        geo_df = df[df['BBL'].isin(geo)]
        
    return geo_df

def inflation_adjustment(df=None):
    for i, row in df.iterrows():
        year = int(df.loc[i]['Yr'])
        df[i:i+1]['IND_WAGES'] = df.loc[i]['IND_WAGES']*(cpi_dict_us[latest_year]/cpi_dict_us[year])
    
    return df

def screen_check(df=None, grouped_df=None, industry='ECONOMIC_SECTOR', target_var='employment', freq_cols=['Yr']):    
    # group establishments by year
    dff = df.groupby(['UID']+freq_cols+industry).agg({'AVGEMP':'mean','TOT_WAGES':'sum'}).reset_index()
    # merge yearly establishments with yearly industries
    df_check = pd.merge(dff, grouped_df, how='left', on=industry+freq_cols)
    # rename variables
    df_check.rename(columns={'AVGEMP_x':'RECORD_EMPL','AVGEMP_y':'IND_EMPL','UID_x':'UID','UID_y':'IND_EST',
                            'TOT_WAGES_x':'RECORD_WAGES','TOT_WAGES_y':'IND_WAGES'}, inplace=True)
    # create binary variable columns as a pass/fail flag for the 80% rule for each establishment
    df_check['EMP80CHECK'] = np.where(df_check['RECORD_EMPL']<df_check['IND_EMPL']*0.80, 0, 1)
    # group this dataset by industry and year/qtr
    df_ind = df_check.groupby(industry+freq_cols).agg({'IND_EST':'mean','IND_EMPL':'mean','IND_WAGES':'mean',
                                                       'EMP80CHECK':'max'}).reset_index()
    if target_var=='establishments':
        final_df = df_ind[(df_ind['IND_EST']==0)|(df_ind['IND_EST']>=3)]
    else:
        final_df = df_ind[(df_ind['EMP80CHECK']==0)&(df_ind['IND_EST']>=3)]  
        
    return final_df

# make custom - allow fashion industry list, etc.
def custom_inds(df=None,ind_df=None,naics_yr=2017): 
    master_df = pd.DataFrame()
    year_list = list(df['Yr'].astype(int).unique())
    ind_6 = ind_df[ind_df['ind_level']=='naics_6']
    ind_5 = ind_df[ind_df['ind_level']=='naics_5']
    ind_4 = ind_df[ind_df['ind_level']=='naics_4']
    ind_3 = ind_df[ind_df['ind_level']=='naics_3']
        
    for year in year_list:     
        df_yr = df[df['Yr'].astype(int)==year]
        
        if year<2002:
            error = 'not available before 2002'
            return error
        elif year<2007:
            naics_yr = 2002
        elif year<2012:
            naics_yr = 2007
        elif year<2017:
            naics_yr = 2012     
        elif year<2022:
            naics_yr = 2017
        elif year>=2022:
            naics_yr = 2022
        
        dig = 3
        for df_ind in [ind_3, ind_4, ind_5, ind_6]:
            df_ = pd.merge(df_yr, xw_ALL, how='left', left_on=f'NAICS_{str(dig)}', right_on=f'NAICS_{str(naics_yr)[2:]}_{str(dig)}')
            dff = pd.merge(df_, df_ind, how='inner', left_on=f'NAICS_{str(naics_yr)[2:]}_{str(dig)}', right_on=f'NAICS{str(dig)}')
            dff_ = dff[list(df.columns)+['tier1','tier2','tier3']]
            df_fin = df_fin.append(dff_).drop_duplicates()
            dig+=1
        
        master_df = master_df.append(df_fin)
        
#         # NAICS crosswalk based on year
#         df6 = pd.merge(df_yr, xw_ALL, how='left', left_on='NAICS_6', right_on=f'NAICS_{str(naics_yr)[2:]}_6')
#         df4 = pd.merge(df_yr, xw_ALL, how='left', left_on='NAICS_4', right_on=f'NAICS_{str(naics_yr)[2:]}_4')

#         # 6-digit merge with PDR industry list
#         dff6 = pd.merge(df6, ind_6, how='inner', left_on=f'NAICS_{str(map_yr)[2:]}_6', right_on='NAICS6')
#         dff6_ = dff6[list(df.columns)+['tier1','tier2','tier3']]
#         # 4-digit merge
#         dff4 = pd.merge(df4, ind_4, how='inner', left_on=f'NAICS_{str(map_yr)[2:]}_4', right_on='NAICS4')
#         dff4_ = dff4[list(df.columns)+['tier1','tier2','tier3']]
#         # concatenate
#         df_fin = pd.concat([dff6_, dff4_]).drop_duplicates()
#         # append
#         master_df = master_df.append(df_fin)
    
    return master_df


### UNDER CONSTRUCTION:

# def employer_size_cols(df=None, industry_level, target_var, freq, ownership, industry_focus):
#     bins = [0, 11, 101, 1001, np.inf]
#     names = ['0-10','11-100','101-1000','1001+']
#     industry = assign_ind(industry_level)
#     df['Employer_Sizes'] = pd.cut(df['AVGEMP'], bins, labels=names)
#     table = pd.pivot_table(df, values='UID', index=industry, columns=['Employer_Sizes'], aggfunc=np.count).reset_index()
    
#     return table