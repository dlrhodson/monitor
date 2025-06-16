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
import configparser
import pickle
import uuid
from subprocess import check_output, STDOUT, CalledProcessError

def check_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def get_hist(references_dict,this_model,this_experiment):
    
    if this_model in references_dict:
        this_ref_model=references_dict[this_model]
    else:
        if '*' in references_dict:
            this_ref_model=references_dict['*']
        else:
            print("No Default references defined!")
            exit()

    if this_experiment in this_ref_model:
        this_ref_name=this_ref_model[this_experiment]
    else:
        if '*' in this_ref_model:
            this_ref_name=this_ref_model['*']
        else:
            print("No Default references defined!")
            exit()

    print("Reading reference: "+this_ref_name+"...")
    hist=cf.read(this_ref_name+'/*nc')

    print("Data Read")

    field_names=[]
    job_names=[]


    for field in hist:
        if not field.has_property('standard_name'):
            print("No standard_name!")
            #AMOC has no standard name!
            print(field)
        else:    
            field_names.append(field.standard_name)
            job_names.append(field.properties()['variant_label'])

    #get unique list
    field_names_unique=list(set(field_names))
    job_names_unique=list(set(job_names))
    return(hist,field_names_unique)

def parse_string_to_nested_dict(s1):
    nested_dict = {}
    # Split the string based on semicolons
    #initial split is on ;
    if ';' in s1:
        items = s1.split(';')
    else:
        items = s1.split(',')

    for item in items:
        if '(' in item and ')' in item:
            key, rest = item.split('(')
            nested_key = key.strip(':')
            nested_value = parse_string_to_nested_dict(rest[:-1])
            nested_dict[nested_key] = nested_value
        else:
            key, value = item.split(':')
            nested_dict[key.strip()] = value.strip()
    return nested_dict


def read_safely(file_string):
    #another idea
    # read each file - trap any read errors
    files=glob.glob(file_string)
    data=cf.FieldList()
    for file in files:
        try:
            data.append(cf.read(file))
        except Exception as error:
            print("couldn't read "+file+" .. skipping", type(error).__name__)
    return(cf.aggregate(data,relaxed_identities=True))
    
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
        #try to captures small variations, but exclude truncated files!
        if length > 0.95*max_length:
            clean_files.append(files[l_count])
        l_count+=1
    return(clean_files)

    





scratch=sys.argv[1] 
job=sys.argv[2]

#QUEUE=sys.argv[3].replace('_',' ')

# Try to read webhook URL from config file
try:
    config = configparser.ConfigParser()
    config.read('monitor.conf')
    
    references = config.get('main', 'references', fallback='')
    root = config.get('main', 'root', fallback='')
    if root=='':
        print("root not defined in monitor.conf?")
        exit(99)
    webroot=f'{root}/public/monitor'

    plots_queue=config.get('main', 'plots_queue', fallback='')
    if plots_queue=='':
        print("plots_queue not defined!")
        print("Please add e.g. \n plots_queue = \"-p standard --qos=short --account=epoc --mem=6000\"\n to monitor.conf")
        exit(99)
    QUEUE=plots_queue.replace('_',' ')
except (FileNotFoundError, configparser.Error):
    print("No monitor.conf?")
    exit(99)

if references=='':
    references_dict=None
else:
    references_dict= parse_string_to_nested_dict(references)

print("Reading "+str(job))

#this avoids a fail due to corrupt netcdf files

#Let's just ignore any file that causes a read error!
data=read_safely('monitor_index/index_'+str(job)+'*.nc')

this_experiment=''
if data[0].has_property('experiment'):
    this_experiment=data[0].properties()['experiment']
    if ' ' in this_experiment:
        #if 'experiement' contains a space, it is probably a description, not an ID!
        this_experiment=data[0].properties()['experiment_id']

this_model=''
if data[0].has_property('source_id'):
    this_model=data[0].properties()['source_id']

hist=None
field_names_unique=[]
job_names=[]
#only get the references (hist) if we define them!
if references_dict is not None:
    hist,field_names_unique=get_hist(references_dict,this_model,this_experiment)
    
canari_names=[]

for cnames in data:
    canari_names.append(cnames.standard_name)

canari_names_unique=list(set(canari_names))

var_dump=scratch+"/"+job+".bin"

with open(var_dump, "wb") as f:
    pickle.dump([job,field_names_unique,canari_names_unique,data,hist],f)
    
print("Written dump "+var_dump)

n_jobs=str(len(canari_names_unique))

print("Launching plot array..")

uid= uuid.uuid4().hex
job_dir='plot_'
log_dir=scratch+'/../LOGS/'+job_dir
script_dir=scratch+'/../SCRIPTS/'+job_dir

#check exists, and create if not
check_dir(log_dir)
check_dir(script_dir)


#create a batch file to plot a single figure for plot n for <job>
batch_file=script_dir+'/batch_'+job+'_'+uid+'.sh'

with open(batch_file, 'w') as f:
    print("""#!/bin/bash
module load jaspy/3.11/v20240302
""",file=f)
    print ("./plot_timeseries_v7_plots.py "+scratch+" "+job+" "+webroot,file=f)

try:
    output = check_output(['chmod', '+x', batch_file], stderr=STDOUT)
except CalledProcessError as exc:
    print(exc.output.decode())
    

sbatch=f'sbatch {QUEUE} --time=01:00:00 --array=1-{n_jobs} --job-name {job} -o {log_dir}/%A_%a.o -e {log_dir}/%A_%a.e {batch_file}'


print(sbatch)

try:
    output = check_output(sbatch.split(' '), stderr=STDOUT)
except CalledProcessError as exc:
    print(exc.output.decode())

print('Array submitted')
