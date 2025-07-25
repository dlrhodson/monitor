#!/usr/bin/env python

#D: Reads in model output and compute global means for job monitoring
#calculate_means.py job date output_directory
#./calculate_means.py cn134 198801 INDEX
#
#Dan Hodson
#08/Jul/2022

# v4 24/Jan/2023
# Now includes Sea ice area from cice fields
# Plus North and South sea ice area
# Soil moisture (summed over all 4 levels)
# Net TOA (rsdt-rsut-rlut)
# Ocean: T and S volume mean

import cf
import re
import glob
import sys
import numpy as np
import os
import smtplib, ssl
from email.message import EmailMessage
import traceback
import urllib3
import json

#this patches the broken weights_measure function
patch_file='cf_patches.py'
exec(compile(source=open(patch_file).read(), filename=patch_file, mode='exec'))


def fix_time_axis(fieldlist):
    #Check to see if we are using an auxiliary time axis
    has_time_axis = False
    if len(fieldlist) > 0:
        field = fieldlist[0]
        for aux_axis in field.auxiliary_coordinates().values():
            if hasattr(aux_axis, 'standard_name') and aux_axis.standard_name == 'time':
                has_time_axis = True
                #replace time axis with auxiliar axis
        if not has_time_axis:
            #no aux time axis - don't need to do anythinhg
            return
        
        for field in fieldlist:
            #find time axis
            #which have axis defined
            coords_with_axis=[x for x in field.coords().values() if x.has_property('axis')]
            T_axis=[x for x in coords_with_axis if x.axis=='T']
            aux_axis=field.aux('time')
            if len (T_axis)!=1:
                print("Can't find T axis?")
                exit()
            T_axis=T_axis[0]
            if T_axis.has_property('standard_name'):
                T_axis_name=T_axis.standard_name
            else:
                T_axis_name='axis=T'
            field.del_construct(T_axis_name)
            new_T_axis=cf.DimensionCoordinate(data=aux_axis.data,properties=aux_axis.properties())

            field.del_construct('time')
            field.set_construct(new_T_axis)
    return()

def get_NAO(jfm):
    q_jfm=cf.Query("ge", 1)&cf.Query("le", 3)
    q_jfm=q_jfm.addattr("month")
    iceland_box=jfm.subspace(X=cf.wi(-90,60),Y=cf.wi(55,90))
    azores_box=jfm.subspace(X=cf.wi(-90,60),Y=cf.wi(20,55))
    nao_jfm=azores_box.collapse('area: mean',weights=True,squeeze=True)-iceland_box.collapse('area: mean', weights=True,squeeze=True)
    nao_jfm.standard_name='NAO_jfm_box'
    nao_jfm_mean=nao_jfm.collapse('time: mean',group=q_jfm)
    return(nao_jfm_mean)

def get_error():
    exception_type, exception_value, trace = sys.exc_info()
    error_string="Exception type {}".format(exception_type)
    error_string+="Exception value: {}\n".format(exception_value)
    trace_string = "".join(traceback.format_tb(trace))
    error_string+=trace_string
    error_string+="\n\n"+data_dir+"\n"+job+"\n"+transfer_dir
    subject="Error report for {} {}\n".format(job,date)
    return(subject+error_string)

# Send Slack notification based on the given message
def slack_notification(message):
    # Try to read webhook URL from config file
    try:
        import configparser
        config = configparser.ConfigParser()
        config.read('monitor.conf')
        webhook_url = config.get('slack', 'webhook_url', fallback='')
    except (FileNotFoundError, configparser.Error):
        webhook_url = ''
    
    # Return early if webhook URL is empty or not defined
    if not webhook_url:
        return
    
    try:
        slack_message = {'text': message}

        http = urllib3.PoolManager()
        response = http.request('POST',
                                webhook_url,
                                body = json.dumps(slack_message),
                                headers = {'Content-Type': 'application/json'},
                                retries = False)
    except:
        traceback.print_exc()

    return True

def report_error():
    message=get_error()
    print(message)
    slack_notification(message)


def read_cice(patterns):
    #read in cice files
    files=[]
    #loop over all pattern is comma separated list
    for pattern in patterns.split(','):
        files.extend(glob.glob(data_dir+'/*'+pattern+'*'))

    if len(files)>0:
        data=cf.read(files)
        return(data)
    else:
        return(0)

def read_ocean(stream,patterns):
    #Read in All ocean files
    files=[]

    #loop over all pattern is comma separated list
    for pattern in patterns.split(','):
        files.extend(glob.glob(data_dir+'/*'+pattern+'*'+stream+'*'))

    if len(files)>0:
        data=cf.read(files)
        return(data)
    else:
        return(0)

def read_monthly_atm(patterns):

    #read in atmosphere files for a particular stream
    files=[]
    #loop over all pattern is comma separated list
    for pattern in patterns.split(','):
        files.extend(glob.glob(data_dir+'/*'+pattern+'*'))


    if len(files)>0:
        data=cf.read(files)
        return(data)
    else:
        return(0)

def read_streams(streams):
    #read in atmosphere files for a particular stream
    files=[]
    for stream in streams:
        #now we match for *a_<NUMBER>_<STREAM>__*
        #Number =0-99
        #stream = mon day 1hr
        #this exludes all the other monthly, daily and hourly files
        files.extend(glob.glob(data_dir+'/*a_'+stream+'_1*'))
     
    if len(files)>0:
        data=cf.read(files)
        return(data)
    else:
        return(0)

def get_ocean(ocean_variables,patterns):
    ###OCEAN
    ocean_list=cf.FieldList()
    print("Reading Ocean Data..")

    #Loop over all grids
    for grid in ocean_variables:
        print(grid)
        #Read in all data from this grid
        data_ocean=read_ocean(grid,patterns)

        if data_ocean==0:
            print("Ocean "+grid+" data missing??")
            exit(99)
        #We'll treat diaptr (AMOC) differently
        if not 'diaptr' in grid:


            #First we need to compute the cell volume from the
            #cell_area and cell-thickness
            #cell_thickness varies in time, so need to extract for all times
            
            
            #use thkcello, as the standard_name cell_thickness can be used for
            #other diagnostics
            
            cell_thickness=data_ocean.select_by_ncvar('thkcello')
            if len(cell_thickness)==0:
                print("No cell thickness! Cannot compute ocean means")
                exit()
                
            
            cell_area=data_ocean.select('cell_area')
            if len(cell_area)==0:
                #no explicit cell area diagnostic
                #does cell_thickness have a cell_area?

                if hasattr(cell_thickness[0], 'cell_measures'):
                    # Get cell measures dictionary
                    measures = cell_thickness[0].cell_measures()
                    # Check if 'area' key exists in cell_measures
                    cell_area_found=False
                    for measure in measures.values():
                        if 'area' in measure.identity():
                            cell_area=measure.data
                            cell_area_found=True
                    if not cell_area_found:
                        print("No cell area measure found in cell_thickness")
                        exit()

            cell_volume_measure=compute_cell_volume_measure(cell_thickness,cell_area)

        
        these_variables=ocean_variables[grid]
        for variable in these_variables:
            if not 'diaptr' in grid: 
                print("Global mean of "+variable)
                data_ocean_var=data_ocean.select(variable)
                if len(data_ocean_var)==0:
                    print("No data for "+variable)
                    exit(99)
                #Fix time axis, if necessary
                fix_time_axis(data_ocean_var)

    
                ocean_index=ocean_depth_mean(data_ocean_var,cell_volume_measure)  
                ocean_list.append(ocean_index)
            else:

                print("Process diaptr")
                if 'meridional_streamfunction_atlantic' in variable:
                    ocean_index=get_amoc_45N(data_ocean)
                    ocean_list.append(ocean_index)

    return(ocean_list)

def get_ice(ice_patterns):
    ice_list=cf.FieldList()
    print("Reading sea ice files")
    sea_ice_data_monthly=read_cice(ice_patterns)


    if sea_ice_data_monthly==0:
        print("No CICE data!")
        exit(99)

    aice1=cf.aggregate(sea_ice_data_monthly.select_by_ncvar('aice'),relaxed_identities=True)
    if len(aice1)>1:
        print("Sea Ice aggregation failed")
        print(aice1)
        exit(99)
    aice=aice1[0]
    #are cell_measures defined?
    has_cell_area=False
    if hasattr(aice,'cell_measures'):
        for measure in aice.cell_measures().values():
            if 'area' in measure.identity():
                has_cell_area=True

    #if no cell area is defined - try and extract from the tarea varible
    if not has_cell_area:
        cell_area=cf.CellMeasure(data=sea_ice_data_monthly.select_by_ncvar('tarea')[0])
        cell_area.units='m2'
        cell_area.measure='area'
        aice.set_construct(cell_area)
    
    aice.standard_name='sea_ice_area_fraction'
    variable_area=area_integral_seaice(aice,job)
    ice_list.extend(variable_area)
    return(ice_list)



def get_atm(atm_variables,atm_patterns):
    atm_list=cf.FieldList()
    print("Reading Atmosphere Data")

    no_data=True

    print("Reading files monthly ATM ")
    data_monthly=read_monthly_atm(atm_patterns)

    if data_monthly==0:
        print("No Monthly ATM data")
    else:
        no_data=False

    if no_data:
        print("No atmospheric data!")
        exit(99)


    #select only full monthly means, averaged over all time steps
    #select fields that are monthly means over either 900s (sea ice/ocean fields) or 3600s (radiation timesteps)
    monthly_means=data_monthly.select_by_property('and',online_operation='average',interval_write='1 month')

    if len(monthly_means)==0:
        #that didn't work
        #let's try lbtim and lbproc
        #lbproc=128= time mean
        #lbtim=1 hr, 360_day
        monthly_means=data_monthly.select_by_property('and',lbtim='122',lbproc='128')
        if len(monthly_means)==0:
            print("Failed to find any ATM fields!")
            exit(99)
            

    
    for variable in atm_variables:

        var_str=str(variable).rjust(5,'0')
        stash_code='m01s'+var_str[:-3]+'i'+var_str[-3:]


        select_variable=monthly_means.select_by_ncvar(re.compile(stash_code))
        found_flag=True
        if len(select_variable)==0:
             print('No entry for '+stash_code)
             found_flag=False
#            print('No entry for '+stash_code+'  checking daily..')
#            select_variable_daily=data_daily.select_by_ncvar(re.compile(stash_code))
#            if len(select_variable_daily)==0:
#                print('No entry for '+stash_code+'  in daily data checking hourly')
#                select_variable_hourly=data_hourly.select_by_ncvar(re.compile(stash_code))
#                if len(select_variable_hourly)==0:
#                    print('No entry for '+stash_code+'  in hourly data ')
#                    found_flag=False
#                else:
#                    print(stash_code+' found in hourly data')
#                    select_variable=cf.FieldList()
#                    print("Converting to monthly means")
#                    for field in select_variable_hourly:
#                        select_variable.append(field.collapse('time: mean',group=cf.M()))
#            else:
#                print(stash_code+' found in daily data')
#                select_variable=cf.FieldList()
#                print("Converting to monthly means")
#
#                for field in select_variable_daily:
#                    select_variable.append(field.collapse('time: mean',group=cf.M()))


        else:
            select_variable_ag=cf.aggregate(select_variable,relaxed_identities=True)
            if len(select_variable_ag) >1:
                print(select_variable_ag[0].standard_name+" has more than one entry - selecting the first occurrence")
                select_variable_ag=select_variable_ag[0]
            
        if found_flag:
            this_variable=select_variable_ag[0]
            #if this variable doesn't have a standard name set, use long_name

            if not this_variable.has_property('standard_name'):
                this_variable.standard_name=this_variable.properties()['long_name'].replace(' ','_').replace('/','_').replace(':','_')
            print(stash_code+': '+this_variable.standard_name)


            variable_area_mean=area_mean(this_variable,job)
            atm_list.append(variable_area_mean)
    ##MASS CONTENT OF WATER IN SOIL
    #compute mass_content_of_water_in_soil
    #CMIP6 stores total mass_content_of_water_in_soil
    #which is the sum over the 4 (non-dimensional) levels in the UM/JUL

    soil_moisture=atm_list.select('moisture_content_of_soil_layer')
    if len(soil_moisture)>0:
        print("Soil moisture! Computing sum over layers for CMIP")

        soil_moisture_total=soil_moisture[0].collapse('depth: sum',squeeze=True)
        #need to rename to cmip
        soil_moisture_total.standard_name='mass_content_of_water_in_soil'
        atm_list.append(soil_moisture_total)
    else:
        print("NO SOIL MOISTURE DATA")

    #TOA NET INCOMING FLUX
    rsdt=atm_list.select('toa_incoming_shortwave_flux')
    rsut=atm_list.select('toa_outgoing_shortwave_flux')
    rlut=atm_list.select('toa_outgoing_longwave_flux')

    if len(rsdt)>0 and len(rsut)>0 and len(rlut)>0:
        net_toa=rsdt[0]-rsut[0]-rlut[0]
        net_toa.standard_name='toa_net_incoming_flux'
        atm_list.append(net_toa)
    else:
        print("NOT enough Radiation data for TOA calculation")

    return(atm_list)


def fix_axes(cf_field):
    #loop over all axes - find the ncdim%x and %y and store names
    #sometimes are e.g. ncdim%x_1
    for axis in cf_field.domain_axes(): 
        this_id=cf_field.domain_axis(axis).identity()
        if 'ncdim%x' in this_id:
            ncdim_x=this_id
        if 'ncdim%y' in this_id:
            ncdim_y=this_id


        #loop over all auxillary coords and set a dimension coord
    for aux in cf_field.auxs():
        aux_name=cf_field.aux(aux).standard_name
        if aux_name=='latitude':
            #define regular CF dimension 
            YY_size=cf_field.domain_axis(ncdim_y).get_size()
            YY=cf.DimensionCoordinate(properties={'axis':'Y','standard_name':'Y'},
                                      data=cf.Data(range(YY_size)))
            cf_field.set_construct(YY)
                
        elif aux_name=='longitude':
            #define regular CF dimension 
            XX_size=cf_field.domain_axis(ncdim_x).get_size()
            XX=cf.DimensionCoordinate(properties={'axis':'X','standard_name':'X'}
                                      ,data=cf.Data(range(XX_size)))
            cf_field.set_construct(XX)
        else:
            print("unknown axis?")
            print(aux_name)
            exit(99)


def compute_cell_volume_measure(cell_thickness_fieldlist,cell_area):
    cell_volume_fieldlist=cf.FieldList()
    for field in cell_thickness_fieldlist:
        cell_volume_np=cell_area.array*field.array
        #create CellMeasure
        cell_volume=cf.CellMeasure(data=cf.Data(np.squeeze(cell_volume_np)))
        cell_volume.units="m3"
        cell_volume.measure="volume"
        cell_volume_fieldlist.append(cell_volume)
    
    return(cell_volume_fieldlist)

def ocean_depth_mean(field_list,cell_measure_volume_list):

    # Zip the field_list and cell_measure_volume_list together
    ocean_depth_mean_list=cf.FieldList()
    for field, cell_volume in zip(field_list, cell_measure_volume_list):
        # Set the cell volume measure for this field
        field.set_construct(cell_volume)
        fix_axes(field)
        ocean_mean=field.collapse('volume: mean', measure=True,squeeze=True)
        ocean_mean.standard_name='global_mean_'+ocean_mean.standard_name
        ocean_depth_mean_list.append(ocean_mean)

    ocean_mean=cf.aggregate(ocean_depth_mean_list)
    return(ocean_mean)


def fix_time_name(field):
    #loop over all coordinates looking for something with 'since' in the units - probably the time!
    for coord in field.coords():
        if 'since' in field.coord(coord).units:
            field.coord(coord).standard_name='time'
            
def area_integral_seaice(field,job):


    #ensure the time axis is labelled correctly!
    fix_time_name(field)



    x_found=False
    y_found=False
    for dim in field.coords().values():
        if 'first dimension' in dim.long_name:
            dim.axis='X'
            x_found=True
        if 'second dimension' in dim.long_name:
            dim.axis='Y'
            y_found=True

    #UKSESM doesn't necessarily have dimension axes defined
    if not x_found:
        ni=field.domain_axes().filter_by_ncdim('ni')
        if len(ni)==0:
            print("Can't find NI axis")
            exit(99)
        ni_size=ni.value().size
        
        XX=cf.DimensionCoordinate(properties={'axis':'X','standard_name':'X'},data=cf.Data(range(ni_size)))
        field.set_construct(XX)

    if not y_found:
        nj=field.domain_axes().filter_by_ncdim('nj')
        if len(nj)==0:
            print("Can't find NJ axis")
            exit(99)
        nj_size=nj.value().size
        YY=cf.DimensionCoordinate(properties={'axis':'Y','standard_name':'Y'},data=cf.Data(range(nj_size)))
        field.set_construct(YY)
            
        


    #fix auxillary dimensions standard_names
    dimensions=['longitude','latitude']
    for aux in field.auxs():
        for dimension in dimensions:
            if dimension in field.aux(aux).long_name:
                field.aux(aux).standard_name=dimension
        
    #set cells with zero area to missing value - important for integration method
 
    #sometimes the cf cice field gets confused and has two cell_measures!
    #one implicit in the sea ice file, and one external, added by XIOS
    #we only want the implicit internal measure
    #this is a hack, but it works - select the measure that DOES NOT have units 
    #and delete - this SHOULD work

    all_area_measures=field.constructs.filter_by_measure('area')
    for measure in all_area_measures:
        #if this area measure does NOT have any units, it must be the external measure - we want to delete this
        this_measure=all_area_measures[measure]
        if not this_measure.has_property('units'):
            field.del_construct(measure)


    #measure=field.cell_measure()
    measure0=field.constructs.filter_by_property(units='m2')
    if len(measure0)==0:
        measure0=field.constructs.filter_by_property(units='m^2')
        if len(measure0)==0:
            print("Can't find measure units!")
            exit(99)
    measure=measure0.value()

    m_area=measure.array
    area_masked=np.ma.masked_array(m_area,mask=m_area==0)
    field.cell_measure().data[:]=cf.Data(area_masked,units='m^2')
    ## NEED TO FIX THIS


    integrals=cf.FieldList()
    integral=field.collapse('area: integral',weights='area',measure=True,squeeze=True)
    #convert to Mega m^2 (10^12 m^2)
    integral.units='Mm2'
    integral.set_properties({'job': job})
    integral.standard_name='global_sea_ice_area'
    integrals.append(integral)

    field_N=field.subspace(latitude=cf.gt(0))
    integral_N=field_N.collapse('area: integral',weights='area',measure=True,squeeze=True)
    integral_N.units='Mm2'
    integral_N.standard_name='northern_sea_ice_area'
    integral_N.set_properties({'job': job})
    integrals.append(integral_N)


    field_S=field.subspace(latitude=cf.lt(0))
    integral_S=field_S.collapse('area: integral',weights='area',measure=True,squeeze=True)
    integral_S.units='Mm2'
    integral_S.standard_name='southern_sea_ice_area'
    integral_S.set_properties({'job': job})
    integrals.append(integral_S)


    return(integrals)


def get_amoc_45N(data):
    #compute AMOC at 45N 
    #The diaptr file does not contain information about how the jlines coordinates map onto the mean lattude, so we need to do this manually, depending on the model resolution - estimated by the ncdim%y size
    #Closest jline to 45N is j=2647 for H
    #Closest jline to 45N is j=886 for M
    #Closest jline to 45N is j= for L
    #see /home/users/dlrhodso/CANARI/monitoring/get_amoc_45_jline.sh
    amoc_45_mappings={'332':'251',
                      '1207':'886',
                      '3606':'2647'
                  }

    print("AMOC45")

    data_ocean_var=data.select_by_ncvar('zomsfatl')
    if len(data_ocean_var)==0:
        print("No zomsfatl for AMOC!")
        exit(99)


    #find jline at 45N
    ysize=str(data_ocean_var[0].coord('latitude').array.shape[0])
    if not ysize in amoc_45_mappings:
        print("Unrecognised model resolution when computing AMOC 45N?")
        print(ysize)
     
        exit()
    amoc_45_jline=int(amoc_45_mappings[ysize])


    #need to reduce x axis for GC3.1 but not UKESM ere
    reduce_x_axis=True
    if data_ocean_var[0].axis('ncdim%x').size==1:
        reduce_x_axis=False


        
    #remove unnecessary x axis
    data_ocean_var_new=cf.FieldList()
    for field in data_ocean_var:
        new_field=field.squeeze('ncdim%x')
        new_field.del_construct('longitude')
        new_field.del_construct('latitude')
        #latitude aux axis is not helpful here
        new_field.del_construct('ncdim%x')
        data_ocean_var_new.append(new_field)
        
    #Fix time axis, if necessary                                                                               
    fix_time_axis(data_ocean_var_new)

    amoc1=cf.aggregate(data_ocean_var_new,relaxed_identities=True)
    if len(amoc1)>1:
        print("Amoc aggregation failed")
        print(amoc1)
        exit(99)
    amoc=cf.FieldList()

    #remove the auxiliary time axis - prevents aggregation
    #ysize=str(amoc1[0].coord('latitude').array.shape[0])
    #if not ysize in amoc_45_mappings:
    #    print("Unrecognised model resolution when computing AMOC 45N?")
    #    print(ysize)
    # 
    #    exit()
    #amoc_45_jline=int(amoc_45_mappings[ysize])

    squeeze_axes=[ x.identity() for x in amoc1[0].domain_axes().values() if not 'time' in x.identity()]
    if reduce_x_axis:
        amoc_m1=amoc1[0][:,:,amoc_45_jline,:].array.squeeze()
        #find the first i that has a non-masked value along this jline
        first_non_masked_i=np.ma.flatnotmasked_edges(amoc_m1)[0]
        #get list of domain axes to squeeze (all but a time axis)
        amoc_m=amoc1[0][:,:,amoc_45_jline,first_non_masked_i].collapse('depth: maximum').squeeze(squeeze_axes)
    else:
        amoc_m=amoc1[0][:,:,amoc_45_jline].collapse('depth: maximum').squeeze(squeeze_axes)


    #Closest jline to 45N is j=885
    amoc_45=cf.Field()
    amoc_45.set_construct(cf.DomainAxis(amoc_m.shape[0]))
    amoc_45.set_data(amoc_m)
    amoc_45.units='Sv'
    amoc_45.set_construct(amoc1[0].coord('time'))
    amoc_45.standard_name='amoc_45n'
    amoc_45.nc_set_variable('amoc45n')    
    amoc_45.set_properties({'job': job})
    return(amoc_45)

    
def area_mean(field,job):
    x_bounds=field.coord('X').create_bounds()
    y_bounds=field.coord('Y').create_bounds()
    field.coord('X').set_bounds(x_bounds)
    field.coord('Y').set_bounds(y_bounds)
    area=field.weights('area')
    mean=field.collapse('area: mean',weights=area,squeeze=True)
    mean.set_properties({'job': job})
    return(mean)




try:
    #sent from PUMA/CYLC 
    cylc_version=os.getenv('CYLC_VERSION')
    if cylc_version==None:
        print("CYLC_VERSION env variable not defined!")
        exit()
    if int(cylc_version.split('.')[0])<8:
        cylc_name=os.getenv('CYLC_SUITE_NAME')
    else:
        cylc_name=os.getenv('CYLC_WORKFLOW_NAME')


    #Transfer dir on JASMIN
    transfer_dir=os.environ['TRANSFER_DIR']+'/'+cylc_name

    #get runid from cylc_suite_name
    job=cylc_name.split('-')[-1]

    #cylc_task_cycle_time
    date=os.environ['CYLC_TASK_CYCLE_POINT']

    #Directory to write the index file to
    #out_dir=os.environ['INDEX_DIR']
    out_dir='monitor_index'
    # Create the output directory if it doesn't exist
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
        print(f"Created output directory: {out_dir}")

    data_dir=transfer_dir+'/'+date

    atm_patterns=os.environ['ATM_PATTERNS']
    ice_patterns=os.environ['ICE_PATTERNS']
    ocn_patterns=os.environ['OCN_PATTERNS']

    ocn_t_grid=os.environ['OCN_T_GRID']
    ocn_diaptr=os.environ['OCN_DIAPTR']

    
    #no MSLP in 1m? 16222

    outlist=cf.FieldList()
    atm_variables=[1201,1207,1208,1209,1210,1211,1235,2201,2204,2205,2206,2207,2208,3217,3223,3225,3226,3232,3234,3236,3237,3245,3317,4204,5205,5206,5215,5216,23,24,409,8023,8208,8209,8223,8225,8234,4203,16222]
    #ocean_variables={'grid_T':['sea_water_potential_temperature','sea_water_salinity'],'diaptr':['meridional_streamfunction_atlantic']}

    ocean_variables={ocn_t_grid:['sea_water_potential_temperature','sea_water_salinity'],ocn_diaptr:['meridional_streamfunction_atlantic']}

    outfile=out_dir+'/index_'+job+'_'+date+'.nc'

    print('Opening job '+job+' date: '+date)


    #Ocean
    outlist.extend(get_ocean(ocean_variables,ocn_patterns))
    #Ice
    outlist.extend(get_ice(ice_patterns))
    #Atm
    outlist.extend(get_atm(atm_variables,atm_patterns))
    

    print("Writing "+outfile)
    cf.write(outlist,outfile)
    print("Done ")

except:
    print("An error happened!")
    report_error()
    exit(99)



