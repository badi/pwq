#!/usr/bin/env python

import matplotlib.pyplot as plt
import pandas as pd

import collections
import os, sys, argparse



def load_task_stats(path, compression='gzip'):
    df = pd.read_table(path, delimiter=',', compression=compression)
    return df

def load_master_stats(path):
    df = pd.read_table(path, delim_whitespace=True)
    df.timestamp -= df.timestamp[0]
    return df

def plot(xs, frame, attrs):
    for a in attrs:
        ys = getattr(frame, a)
        plt.plot(xs, ys)

