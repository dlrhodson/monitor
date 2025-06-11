#!/bin/bash

if [[ ! -e monitor.conf ]]
then
    echo "monitor.conf does not exist"
    echo "create a monitor.conf file defining the root directory"
    echo "e.g. root=/gws/nopw/j04/topproject"
    exit
fi

#root is defined in monitor.conf
gws=$(grep "^root=" monitor.conf | cut -d'=' -f2)

if [[ ! -e $gws ]]
then
    echo "$gws does not exist"
    exit 1
fi


gws=$1
monitor="$gws/monitor"
public="$gws/public/monitor"

#Setup monitor working directory
if [[ -e $monitor ]]
then
    echo "$monitor already exists"
else
    echo "mkdir -P $monitor"
    echo "$monitor created"
fi

#setup public web directory
if [[ -e $public ]]
then
    echo "$public already exists"
else
    echo "mkdir -P $public"
    echo "$public created"
fi
fullp=${public%/*}
echo -e "please email support@jasmin.ac.uk to ask that $fullp is made web accessible"

#link web directory in monitor working directory
echo "ln -s $public $monitor/public"
echo "mkdir -p $monitor/public/IMAGES"
echo "ln -s $monitor/public/IMAGES $monitor/IMAGES"




