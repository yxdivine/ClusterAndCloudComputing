#!/usr/bin/env python

# -*- coding: UTF-8 -*-
import json
import re
from mpi4py import MPI
import time

start_time = time.time()

comm = MPI.COMM_WORLD


# task1: load Grid (load: light)
def load_grid(grid_path):
    ret = []
    with open(grid_path,"r") as json_data:
        data = json.load(json_data)
        for x in data['features']:
            if 'properties' in x:
                ret.append(x['properties'])
                # print(x['properties'])
    return ret


def parse_line(line, grids):
    m = re.search('"coordinates":\[([\d.-]+),([\d.-]+)\]', line)
    if m and m.group(1) and m.group(2):
        point = [float(m.group(2)),float(m.group(1))]
        return compare_block(point,grids)
    return None


def compare_block(point, grids):
    for grid in grids:
        if in_grid(point,grid):
            return grid['id']
    return None


# check whether the point is in the block.
# One point should only falls into one block, that is the first block that comes to compare
def in_grid(point, grid):
    (px, py) = point
    if px >= grid['xmin'] \
            and px <= grid['xmax'] \
            and py >= grid['ymin'] \
            and py <= grid['ymax']:
        return True
    else:
        return False


# sort the result dictionary in reversed order.
def sort_dict(d):
    return sorted(d.items(), key=lambda x:x[1], reverse=True)


# print a dictionary in certain order.
def print_dict(pformat, d):
    for (k,v) in d:
        print(pformat % (k,v))
    print(" ")

# start

GridPath = "melbGrid.json"
InstaPath = "tinyInstagram.json"
# InstaPath = "mediumInstagram.json"
# InstaPath = "bigInstagram.json"


grids = load_grid(GridPath)
grids = load_grid(GridPath)

grid_dict = {}

for grid in grids:
    grid_dict[grid['id']] = 0

with open(InstaPath,"r") as f:
    line_num = 0
    for line in f:
        line_num += 1
        if (line_num % comm.size) == comm.rank:
            belonging = parse_line(line, grids)
            if belonging:
                grid_dict[belonging] += 1
                # print(comm.rank, belonging)


comm.Barrier()   # wait for everybody to synchronize _here_

data = comm.gather(grid_dict)

if comm.rank == 0:
    result = {}
    rows = {}
    cols = {}
    # combine data
    for p_dict in data:
        for (k,v) in p_dict.items():
            if not result.get(k):
                result[k] = 0
            result[k] += v
    # parse rows and columns
    for (k,v) in result.items():
        if not rows.get(k[0]):
            rows[k[0]] = 0
        rows[k[0]] += v
        if not cols.get(k[1]):
            cols[k[1]] = 0
        cols[k[1]] += v
    print_dict("%s: %d posts", sort_dict(result))
    print_dict("%s-Row: %d posts", sort_dict(rows))
    print_dict("Column %s: %d posts", sort_dict(cols))

    print("Used time:",time.time() - start_time,"(s)")
