#!/usr/bin/env python
import csv
import os
import re

DIR_NAME = "results_csv"
PROJ_NAME = "results_main"
RESULTS_FILE = "benchmark_results.txt"


def parse_args(line):
    try:
        bench_result = line.split()
        if len(bench_result) < 4:
            raise ValueError
        title = bench_result[0].rsplit('/', 1)
        if len(title) < 2:
            raise ValueError
        test = title[0].replace("/", "")
        count = re.search(r'\d+', title[1])
        if count is None:
            raise ValueError
        count = int(count.group(0))
        time_ind = bench_result.index("ns/op") - 1
        if time_ind <= 0:
            raise ValueError
        time = float(bench_result[time_ind])
        return test, [count, time]
    except ValueError:
        return None, None


try:
    os.mkdir(DIR_NAME, mode=0o777)
except FileExistsError:
    pass

try:
    os.mkdir(DIR_NAME + "/" + PROJ_NAME, mode=0o777)
except FileExistsError:
    pass

with open(RESULTS_FILE) as results:
    line = '#'
    name, csv_line = None, None
    while line != '':
        while line != '' and name is None:
            line = results.readline()
            name, csv_line = parse_args(line)
        if line != '':
            with open(DIR_NAME + "/" + PROJ_NAME + '/' + name + ".csv", 'w') as f:
                writer = csv.writer(f, delimiter=',')
                csv_name = name
                writer.writerow(["count", "time"])
                while name is not None and csv_name == name:
                    writer.writerow(csv_line)
                    line = results.readline()
                    name, csv_line = parse_args(line)

        else:
            break
