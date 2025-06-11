#!/usr/bin/env python

#this version plots a given job against the hist or scenario ensemble as required
#it inspects the metadata 'experiment' variable to see whether to plot historical data, or furture scenario data
#This SHOULD plot CANARI scenario indices againts UKESM ssp370 (Blue) and HadGEM-GC3.1 ssp585 (BLACK) indices  
#10/Jul/2023

import cf
import cfplot_fix as cfp
import sys
import os 
import glob
from datetime import datetime
import pickle

def rmfilt_cf(field,n):
    '''
    2n+1 running mean filter for cf field
    '''
    import numpy as np
    data=field.array
    nd=len(data)
    if nd<(2*n+2):
        #too few points to apply running mean filter - so do nothing
        return
    new_array=np.zeros(nd)
    #prefil array with NANS - some elements we never compute the means for
    new_array[:]=np.nan
    
    #loop over each element which the the centre of the complete -n:n range
    for i in range(n,nd-n):
        this_sub=data[i-n:i+n]
        #compute mean
        new_array[i]=this_sub.mean()
    field.data[:]=new_array


def update_webpage(this_job,webroot):
    print("Updating webpage..")
    image_dir=webroot+'/IMAGES'
    files=glob.glob(image_dir+'/*'+job+'-HH*.png')

    webdir="../IMAGES"
    webpagedir=webroot+'/'+job
    if not os.path.exists(webpagedir):
         os.makedirs(webpagedir)
    webpage=webpagedir+'/index.html'
    f=open(webpage,'w')

    f.write("<html><head></head><body>\n")
    f.write("<h1>"+job+"</h1>")
    f.write("<em> Last updated: "+str(datetime.now())+" </em>")
    f.write("<table>\n")
    f.write("<tr>\n")

    rowcount=0
    for file in files:
        fname=file.split('/')[-1]
        title=(fname.split('.')[0]).replace('_',' ')
        f.write("<td style=\"text-align:center; padding-bottom: 80px\"><br><img src=\""+webdir+"/"+fname+"\" width=\"100%\" onclick=\"window.open(this.src)\">"+title)
        f.write("</td>\n")
        rowcount+=1
        if rowcount>2:
            f.write("</tr><tr>\n")
            rowcount=0

    f.write("</tr>\n")
    f.write("</table>\n")
    f.write("</body></html>\n")
    f.close()
    print(".. Done")



def clean_netcdf_files(file_string):
    #removes truncated netcdf files by only keeping netcdf files with the max
    #length in the list - hopefully this should work - unless some bug causes
    #sporadic long files!
    files=glob.glob(file_string)
    clean_files=[]
    file_length=[]
    for file in files:
        #print(file)
        file_length.append(os.path.getsize(file))
    max_length=max(file_length)
    l_count=0
    for length in file_length:
        #if length == max_length:
        #try to captures small variations, but exclude truncated files!
        if length > 0.95*max_length:
            clean_files.append(files[l_count])
        l_count+=1
    return(clean_files)

    
def save_plot(canari,fields,filename,this_job):


    outdir="IMAGES"
    outfile=outdir+'/'+filename+'_'+this_job+'-HH'

    annual_mean_flag=False
    if canari.coord('time').size>60:
        ann_mean=canari.collapse('time: mean',group=cf.Y())
        annual_mean_flag=True
    else:
        ann_mean=canari
        
    #fix canari time units to be same as CMIP
    new_t_units=cf.Units('days since 1850-01-01')
    ann_mean.coord('time').convert_reference_time(inplace=True,units=new_t_units)
    

     
    print("Writing "+outfile+'.png')

    cfp.gopen(file=outfile)


    if len(fields)>0:
        #if the field has units % and the canari units are FRACTION ('1')
        #then change canari uits to % (cf will handle the conversion)
        if fields[0].units=='%' and ann_mean.units=='1':

            ann_mean.units='%'

    
    #only plot line if we have more than one timepoint
    if ann_mean.coord('time').shape[0]>1:
        cfp.lineplot(ann_mean,zorder=2,color='r',linewidth=2,title='Reference: '+reference_name)
        if 'toa_net_incoming_flux' in canari.standard_name:
            rmfilt_cf(ann_mean,5)
            cfp.lineplot(ann_mean,zorder=2,color='r',linewidth=5)
            

    #only plot cmip6 hist field if it exists for this variable
    if len(fields)>0:
        for field in fields:

            field.coord('time').convert_reference_time(inplace=True,units=new_t_units)
            if annual_mean_flag:

                ann_mean=field.collapse('time: mean',group=cf.Y())
            else:
                first_year=field.coord('time')[0].year.array[0]
                if first_year>2014:
                    ann_mean=field.subspace(time=cf.wi(cf.dt('2015-01-01'),cf.dt('2020-01-01')))    
                else:
                    ann_mean=field.subspace(time=cf.wi(cf.dt('1950-01-01'),cf.dt('1956-01-01')))
            #get the model used to produce this field
            this_model=field.properties()['source_id']
            if 'HadGEM3' in this_model:
                cfp.lineplot(ann_mean,zorder=1,color='k')
                if 'toa_net_incoming_flux' in ann_mean.standard_name:
                    rmfilt_cf(ann_mean,5)
                    cfp.lineplot(ann_mean,zorder=2,color='k',linewidth=5)
            else:
                #must be UKESM
                cfp.lineplot(ann_mean,zorder=1,color='b')
                if 'toa_net_incoming_flux' in ann_mean.standard_name:
                    rmfilt_cf(ann_mean,5)
                    cfp.lineplot(ann_mean,zorder=2,color='b',linewidth=5)
                
    cfp.gclose()

    return()

def canari_select(canari_data,name): 
    canari_field1=canari_data.select(name)
    if len(canari_field1)>1:
        #didn't aggregate on reading? Try relaxed_identities
        canari_field2=cf.aggregate(canari_field1,relaxed_identities=True)
        if len(canari_field2)>1:
            print(canari_name+" didn't aggregate!")
            exit()

        else:
            canari_field=canari_field2[0]
    else:
        canari_field=canari_field1[0]
    return(canari_field)


def canari_sub_name(name,index):
    #creates sub_name for canari field with multiple levels
    
    if 'SURFACE_TILE_FRACTIONS' in name:
        surface_tile_types=['Broadleaf_tree','Needleleaf_tree','C3_grass','C4_grass','Shrub','Urban','Water','Bare_Soil','Ice']
        new_name=name+'_'+surface_tile_types[index]+'_'+str(index)

    else:
        new_name=name+'_'+str(index)
 
    return(new_name)

scratch=sys.argv[1]
job=sys.argv[2]
webroot=sys.argv[3]
max_plot_number=int(os.environ['SLURM_ARRAY_TASK_MAX'])-1
plot_number=int(os.environ['SLURM_ARRAY_TASK_ID'])-1
var_dump=scratch+"/"+job+".bin"

with open(var_dump, "rb") as f:
    job,field_names_unique,canari_names_unique,data,hist=pickle.load(f)

#get the reference name from the hist data
reference_name=hist[0].get_filenames().pop().split('/')[-2]
canari_names_unique.sort()
field_names_unique.sort()
    
for i in range(len(canari_names_unique)):
    print(i,":",canari_names_unique[i])


for i in range(len(field_names_unique)):
    print(i,":",field_names_unique[i])


for canari_name in [canari_names_unique[plot_number]]:
    #does this field exist in the historical data?
    if canari_name in field_names_unique:
        print(canari_name+" exists in historical data")
        hist_fields=hist.select(canari_name)
        canari_field=canari_select(data,canari_name)
        save_plot(canari_field,hist_fields,canari_name,job)
    else:
        print(canari_name+" doesn't exists in historical data")
        canari_field=canari_select(data,canari_name)
        canari_shape=canari_field.data.shape
        if len(canari_shape)>1:
            #this field has extra non-time dimensions!
            if len(canari_shape)>2:
                print(canari_name+"  has more than 2 dimensions - not sure I know how to handle that!")
                exit()
            else:
                #loop over the 2nd dimension
                for second_dimension in range(canari_shape[1]):
                    #We assume that time is the first dimension here!
                    canari_subspace=canari_field[:,second_dimension].squeeze()

                    save_plot(canari_subspace,[],canari_sub_name(canari_name,second_dimension),job)
                    
        else:
            save_plot(canari_field,[],canari_name,job)

#only update the webpages if we are the last plot            
if plot_number==max_plot_number:   
    print("Updating webpages...")
    update_webpage(job,webroot)
       
print("Done")    
exit()

#Notes:
#http://ajheaps.github.io/cf-plot/multiple_plots.html
#Notebooj?
#https://help.jasmin.ac.uk/article/4851-jasmin-notebook-service

#https://notebooks.jasmin.ac.uk/user/dlrhodso/lab
