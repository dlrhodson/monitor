#!/bin/bash

#root is defined in monitor.conf
root=$(grep "^root=" monitor.conf | cut -d'=' -f2)

monitor=$root/monitor
webroot=$root/public/monitor

#references="'HadGEM3-GC31-LL:(control-1950:GC3.1_LL_1950s_control,highres-future:GC3.1_LL_SSP585,highres-future-noghg:GC3.1_LL_SSP585,hist-1950:GC3.1_LL_1950s_hist,hist-1950-noghg:GC3.1_LL_1950s_hist,*:GC3.1_LL_1950s_control);HadGEM3-GC31-HH:(control-1950:GC3.1_HH_1950s_control,highres-future:GC3.1_HH_SSP585,highres-future-noghg:GC3.1_HH_SSP585,*:GC3.1_HH_1950s_control);*:(control-1950:GC3.1_HH_1950s_control,highres-future:GC3.1_HH_SSP585,*:GC3.1_HH_1950s_control)'"

#references are defined in monitor.conf
references=$(grep "^references=" monitor.conf | cut -d'=' -f2)

echo $references

cd $monitor
jobs=$(ls -1 monitor_index/*_?????_* |while read r;do r0=${r##*/};r2=${r0#*_};echo ${r2%_*};done|uniq)

TAG='plot_'
JOBTIME="02:00:00"

LOTUS="/home/users/dlrhodso/bin/LOTUS2 +jaspy_version jaspy/3.11/v20240302 -queue short-serial-4hr"
plots_queue="-p standard --qos=short --account=epoc --mem=6000"

echo $jobs

for job in $jobs
do
    echo $job
    $LOTUS  -mem 8000 $TAG $job $JOBTIME plot_timeseries_v7.py $job $webroot $references ${plots_queue// /_}
 
done

exit
