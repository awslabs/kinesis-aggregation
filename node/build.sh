#!/bin/bash

# KPL Deaggregator Module Sample Lambda Function

# create the readme file
cat node_modules/kpl-deagg/README.md > README.md
cat BuildForLambda.md >> README.md

# create the lambda function zip
version=`cat package.json | grep version | cut -d: -f2 | sed -e "s/\"//g" | sed -e "s/ //g" | sed -e "s/\,//g"`

functionName=kpl-deagg-sample
filename=$functionName-$version.zip
region=eu-west-1

rm $filename 2>&1 >> /dev/null

zip -r $filename *.js package.json node_modules/ README.md && mv -f $filename dist/$filename

if [ "$1" = "true" ]; then
  aws lambda update-function-code --function-name $functionName --zip-file fileb://dist/$filename --region $region
fi
