#!/bin/sh

i=0
num=64

while [ $i -lt $num ]
do
    ./gensort -a 330000 "partition"$i;
    i=`expr $i + 1`
done