import pandas as pd
import geopandas as gpd
import numpy as np
import qcew_operations as op

# Map ownership codes to descriptions
own_codes = {1:'public', 2:'public', 3:'public', 5:'private'}
latest_year = 2022
latest_qtr = 'Q3'
# Map CPI values to years for inflation adjustment
# from BLS website R-CPI-U: https://www.bls.gov/cpi/research-series/r-cpi-u-rs-home.htm
cpi_dict_us = {2000:172.2, 2001:177.1, 2002:179.9, 2003:184.0, 2004:188.9, 2005:195.3, 2006:201.6, 2007:207.342, 2008:215.303,
               2009:214.537, 2010:218.056, 2011:224.939, 2012:229.594, 2013:232.957, 2014:236.736, 2015:237.017, 2016:240.007,
               2017:245.12, 2018:251.107, 2019:255.657, 2020:258.811, 2021:270.97, 2022:292.655}
# Map NAICS 2-digit sectors to Macro Sectors
macro_dict = {'Mining, Quarrying, and Oil and Gas Extraction':'Industrial','Professional, Scientific, and Technical Services':'Office',
                  'Unclassified':'Local Services','Accommodation and Food Services':'Local Services','Retail Trade':'Local Services',
                  'Other Services (except Public Administration)':'Local Services','Construction':'Industrial',
                  'Real Estate and Rental and Leasing':'Office',
                  'Administrative and Support and Waste Management and Remediation Services':'Office','Information':'Office',
                  'Manufacturing':'Industrial','Wholesale Trade':'Industrial','Health Care and Social Assistance':'Institutional',
                  'Transportation and Warehousing':'Industrial','Educational Services':'Institutional',
                  'Arts, Entertainment, and Recreation':'Local Services','Management of Companies and Enterprises':'Office',
                  'Finance and Insurance':'Office','Utilities':'Industrial','Agriculture, Forestry, Fishing and Hunting':'Industrial',
                  'Public Administration':'Government','Mining, Quarrying, and Oil and Gas Extraction':'Industrial'}
# _______________________________________________________________________________________________________________________________________ #

### OUTPUT FUNCTIONS ###

# Returns table sorted by industry level and timeframe of interest, filtered to geography of interest and screened for disclosure
def table_output(# input data
                 df=None,
                 # input shapefile (area of interest)
                 shapefile=None,
                 # input area of interest - list of geos
                 geo=None,
                 # 'cd', 'tract', 'BBL', 'nta', etc.
                 geo_level='cd',
                 # '2 digit', '3 digit' or '4 digit' (includes preceeding levels), 'macro sector', 'TOTAL', or 'PDR' (industrial)
                 industry_level='2 digit',
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
    industry = op.assign_ind(industry_level)
    target = op.assign_targ(target_var)
    freq_cols = op.assign_freq_cols(freq)
    
    # Clean data
    dff = op.clean_data(df, ownership, industry_focus, time_frame, quarters, function='table')
    
    if industry_level[:3]=='PDR':
        dff = op.pdr_inds(dff.copy())
    
    if geo==None:
        # Geopandas spatial join
        geo_df = op.spatial_join(dff, shapefile)
    else:
        # Shapeless spatial filter
        geo_df = op.shapeless_geo_filter(dff, geo_level, geo)
    
    # Group data by industry
    grouped_df = geo_df.groupby(industry+freq_cols).agg({'UID':'count','AVGEMP':'sum','TOT_WAGES':'sum'}).reset_index()
    
    # Screening
    if screen==True:
        final_df = op.screen_check(geo_df, grouped_df, industry, target_var, freq_cols)
    elif screen==False:
        grouped_df.rename(columns={'AVGEMP':'IND_EMPL','UID':'IND_EST','TOT_WAGES':'IND_WAGES'}, inplace=True)
        final_df = grouped_df.copy()
    
    # Convert establishment and employment averages to integers
    final_df[['IND_EST','IND_EMPL']] = np.round_(final_df[['IND_EST','IND_EMPL']])
    
    # Adjust all wage totals for inflation:
    if target_var == 'wages':
        final_df_ = op.inflation_adjustment(final_df)
    
    # pivot for final output form
    table = pd.pivot_table(final_df, values=target, index=industry, columns=freq_cols, margins=False)
    
    return table
    
    # REVISIT INFLATION ADJUSTMENT FOR WAGES
    
    
# Returns raw records (NOT SCREENED) to private drive, filtered by geography, industry, timeframe of interest 
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
    dff = clean_data(df, ownership, industry_focus, time_frame, quarters, function='records')
    
    # Filter to area of interest
    if geo==None:
        # Geopandas spatial join
        geo_df = spatial_join(dff, shapefile, time_frame)
    else:
        # Shapeless spatial filter
        geo_df = shapeless_geo_filter(dff, geo_level, geo)
    
    return geo_df