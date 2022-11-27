#!/bin/bash

i=0
num=64

while [ $i -lt $num ]
do
    ./gensort -a 330000 "partition"$i;
    mv "partition"$i /home/data;
    i=`expr $i + 1`
done
