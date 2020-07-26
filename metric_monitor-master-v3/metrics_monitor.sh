#!/bin/bash

basedir=$(cd `dirname $0`; pwd)
while true
do
    python ${basedir}/Main.py
    sleep 1800
done


#kubectl -n tdh602test patch instance tdh602test-ci--inceptor  --patch '[{"op": "replace", "path": "/spec/configs/App/executor/replicas", "value":"4"}]' --type='json'