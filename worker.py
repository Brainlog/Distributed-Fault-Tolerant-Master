import logging
import os
import re
import sys
from typing import Any
import time
import pandas as pd
from redis.client import Redis
import glob
from english import load_words
from base import Worker
from constants import FNAME, IS_RAFT
from mrds import MyRedis

ADDED = f'added'
MYHASH = f'myhash'

word_set = load_words()

class WcWorker(Worker):

 def parsefile(self, filename):
    df = pd.read_csv(filename)
    ls = list(df['text'])
    dict = {}
    for s in ls:
      if(type(s) == str):
        ls0 = s.split(' ')
        for elem in ls0:
          if elem in dict:
            dict[elem]+=1
          else:
            dict[elem]=1
    return dict  

 def run(self, **kwargs: Any) -> None:
  if IS_RAFT==False:
    rds: MyRedis = kwargs['rds']
    processed_items = 0

    while True:
     try:
      a = rds.read(self)
      id,data = a

      if type(id)==int and id<0:
        # sleep(.2)
        # logging.debug("Did not get a file. Try again.")
        continue

      # logging.debug(f"Got {id} {data}")

      if (processed_items == 25) and self.crash:
        logging.critical(f"CRASHING!")
        sys.exit()

      if (processed_items == 5) and self.slow:
        logging.critical(f"Sleeping!")
        os.system(f"sudo cpulimit -p {self.pid} --limit {self.cpulimit} --background")

      fname: str = data[FNAME].decode()
      

      wc: dict[str, int] = {}
      
      wc = self.parsefile(fname)
      lis = []
      for key,val in wc.items():
        lis.append([val,key])
      lis.sort(reverse=True)
      cw = {}
      for i in range(10):
        cw[lis[i][1]] = lis[i][0]
      while True:
          try:
            rds.write(id, cw, fname)
            break
          except:
            continue
      processed_items += 1
     except:
       continue
  else:
    lock = True
    while lock:
      try:
        rds: MyRedis = kwargs['rds']
        data_dir: str = kwargs['data_dir']
        worker_id: int = kwargs['worker_id']
        processed_items = 0
        mainwc = {}
        def merge_dicts(dict1, dict2):
            merged_dict = dict1.copy()  # Make a copy of dict1 to avoid modifying it in-place
            for key, value in dict2.items():
              if key in merged_dict:
                merged_dict[key] += value
              else:
                merged_dict[key] = value
            return merged_dict
        for iter, file in enumerate(glob.glob(data_dir)):
              try:
                file2 = file.split("/")[-1].split(".")[-2].split("_")[-1]
                fil = int(file2)
              except:
                continue
              if(fil%kwargs['workers_cnt']==worker_id):
                    wc = self.parsefile(file)
                    mainwc = merge_dicts(mainwc,wc)
        lis = []
        for key,val in mainwc.items():
              lis.append([val,key])
        lis.sort(reverse=True)
        cw = {}
        for i in range(10):
            try:
              cw[lis[i][1]] = lis[i][0]
            except:
              continue
        rds.writeraft(cw)
        rds.update_flag()
        return
      except Exception as e:
          e = str(e)
          e = e.split(" ")
          e = e[-1]
          e = e.split(":")
          e = e[-1]
          try:
            e = int(e)
          except:
            continue
          rds = Redis(host='localhost',port=e,db=0)
    
  
                
                
          
  
