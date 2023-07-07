#!/bin/bash
ami=$(echo "$0"|rev|cut -d'/' -f1|rev)
whereami=$(echo "$0"|sed "s/$ami//g")
pbin=$(find $whereami -name 'python3')
sed -i "s@#!.*@#!$pbin@1" $whereami/initconf.py
sed -i "s@#!.*@#!$pbin@1" $whereami/initgeo.py
sed -i "s@#!.*@#!$pbin@1" $whereami/run.py