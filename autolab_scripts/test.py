# Run by driver.sh

import os
import json
import csv

result = {'scores': {}}
scores = {}

def exec_test(executable, test, subtest) -> int:
    return os.system(f'./build/test/{executable} --gtest_filter={test}.{subtest}')

reader = csv.DictReader(open('./autolab_scripts/p1-rubric.csv', 'r'))

for line in reader:
    if exec_test(line['exec'], line['test'], line['subtest']) == 0:
        scores[line['subtest']] = line['score'] 
    else:
        scores[line['subtest']] = 0

result['scores'] = scores

print(json.dumps(result))
    