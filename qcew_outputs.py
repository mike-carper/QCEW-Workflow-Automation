import pandas as pd
import geopandas as gpd
import numpy as np
import qcew_operations as op

own_codes = {1:'public', 2:'public', 3:'public', 5:'private'}
latest_year = 2023
latest_qtr = 'Q1'


### OUTPUT FUNCTIONS ###

def table_output(# input data
                 df=None,
                 # input shapefile (area of interest)
                 shapefile=None,
                 # input area of interest - list of geos
                 geo=None,
                 # 'cd', 'tract', 'BBL', 'nta', etc.
                 geo_level='cd',
                 # 2, 3, 4, 5 or '6 digit', 'macro sector', 'TOTAL', or 'tier1/2/3' (custom industries)
                 industry_level='2 digit',
                 # input industry definition file as dataframe
                 ind_df=None,
                 # naics definition year to be mapped to
                 target_yr=2022,
                 # 'annual' or 'quarterly'
                 freq='annual',
                 # 'employment', 'establishments' or 'wages'
                 target_var='employment',
                 # range(x,y)
                 time_frame=range(2000,2023),
                 # list of quarters to be included
                 quarters=['Q1','Q2','Q3','Q4'],
                 # 'private', 'public' or 'all'
                 ownership='private',
                 # 'all', 'name of industry' or [list of industries]
                 industry_focus='all',
                 # privacy screening: True or False
                 screen=True):
    
    # Assign variables based on inputs
    industry, ind_level = op.assign_ind(industry_level)
    target = op.assign_targ(target_var)
    freq_cols = op.assign_freq_cols(freq)
    
    # Clean data
    dff = op.clean_data(df, ownership, industry_focus, time_frame, quarters, function='table')
    
    # Add custom industry designations if needed
    if type(ind_df) is pd.core.frame.DataFrame:
        dff = op.custom_inds(dff.copy(),ind_df,target_yr)
    else:
        if int(ind_level)>=3:
            if time_frame[0]<2022:
                dff = op.crosswalk(dff.copy(),ind_level,target_yr)
    
    # Filter by area of interest - method depends on inputs
    if type(shapefile) is gpd.geodataframe.GeoDataFrame:
        geo_df = op.spatial_join(dff, time_frame, shapefile)
    else:
        if geo==None:
            geo_df = dff.copy()
        else:
            # Shapeless spatial filter
            geo_df = op.shapeless_geo_filter(dff, geo_level, geo)
    
    # Group data by quarter
    grouped_df = geo_df.groupby(industry+['Yr','Qtr']).agg({'UID':'count','AVGEMP':'sum','TOT_WAGES':'sum'}).reset_index()
    
    if freq=='annual':
        # Group data by year
        grouped_df = grouped_df.copy().groupby(industry+['Yr']).agg({'UID':'mean','AVGEMP':'mean','TOT_WAGES':'sum'}).reset_index()
    
    # Screening
    if screen==True:
        final_df = op.screen_check(geo_df, grouped_df, industry, target_var, freq_cols)
    elif screen==False:
        grouped_df.rename(columns={'AVGEMP':'IND_EMPL','UID':'IND_EST','TOT_WAGES':'IND_WAGES'}, inplace=True)
        final_df = grouped_df.copy()
    
    # Convert establishment and employment averages to integers
    final_df[['IND_EST','IND_EMPL']] = np.round_(final_df[['IND_EST','IND_EMPL']])
    
    # Adjust all wage totals for inflation: REVISE THIS SECTION
    if target_var == 'wages':
        final_df_ = op.inflation_adjustment(final_df)
    
    # pivot for final output form
    table = pd.pivot_table(final_df, values=target, index=industry, columns=freq_cols, margins=False)
    
    return table
    
    
def records_output(# input data
                 df=None,
                 # input shapefile (area of interest)
                 shapefile=None,
                 # 'cd', 'tract', 'BBL', 'nta', etc.
                 geo_level='cd',
                 # input area of interest - list of geos
                 geo=None,
                 # range(x,y)
                 time_frame=range(2000,2023),
                 # list of quarters to be included
                 quarters=['Q1','Q2','Q3','Q4'],
                 # 'private', 'public' or 'all'
                 ownership='private',
                 # 'all' or 'name(s) of industry'
                 industry_focus='all'):
    
    # Clean data
    dff = op.clean_data(df, ownership, industry_focus, time_frame, quarters, function='records')
    
    # Filter to area of interest
    if geo==None:
        # Geopandas spatial join
        geo_df = op.spatial_join(dff, time_frame, shapefile)
    else:
        # Shapeless spatial filter
        geo_df = op.shapeless_geo_filter(dff, geo_level, geo)
    
    return geo_df