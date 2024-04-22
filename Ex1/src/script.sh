#!/bin/bash

file_path="hdfs:///user/dic24_shared/amazon-reviews/full/reviewscombined.json"

python run.py --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar -r hadoop "$file_path"
