from __future__ import annotations

import logging
from typing import Optional, Final

from redis.client import Redis
import os
from base import Worker
from constants import IN, COUNT, FNAME, IS_RAFT, RAFT_PORTS
import time

ADDED = f'added'
MYHASH = f'myhash'
MYHASH2 = f'latency'
FLAG = f'flag'
FLAGSTREAM = f'flagstream'
class MyRedis:
  def __init__(self):
    self.rds: Final = Redis(host='localhost', port=6379, password='',
                       db=0, decode_responses=False)
    if IS_RAFT==False:
      self.rds.flushall()
      self.rds.xgroup_create(IN, Worker.GROUP, id="0", mkstream=True)
      self.rds.is_pending1 = True

  def get_timestamp(self) -> float:
    timestamp = self.rds.time()
    return float(f'{timestamp[0]}.{timestamp[1]}')

  def add_file(self, fname: str) -> None:
      KEYS = [IN,FNAME,fname,MYHASH,str(self.get_timestamp())]
      member = []
      self.rds.fcall('addfile',5,*KEYS,*member)

    

  def top(self, n: int) -> list[tuple[bytes, float]]:
    p = self.rds.zrevrangebyscore(COUNT, '+inf', '-inf', 0, n,
                                     withscores=True)
    return p

  def get_latency(self) -> list[float]:
    lat = []
    lat_data = self.rds.hgetall("latency")
    for k in sorted(lat_data.keys()):
      v = lat_data[k]
      lat.append(float(v.decode()))
    return lat

  def read(self, worker: Worker) -> Optional[tuple[bytes, dict[bytes, bytes]]]:
      entry = self.rds.xreadgroup(Worker.GROUP, worker.pid, {IN: '>'}, count=1)
      chk = 0
      if(len(entry) == 0):
        # print("I am here")
        chk = 1
        pendinglist = self.rds.xpending(IN, Worker.GROUP)
        if(pendinglist['pending']==0):
          return (-1,-1)
        start = pendinglist['min']
        entry = self.rds.xautoclaim(IN,Worker.GROUP,worker.pid,10000,start_id=start,count=1)
        if(len(entry[1])==0):
          return (-1,-1)
      filepath = ""
      mid = 0
      if chk == 0:
        filepath = entry[0][1][0][1]
        m_id = entry[0][1][0][0]
        keys= [MYHASH,filepath[FNAME].decode(),self.get_timestamp()]
        memu = []
        self.rds.fcall('reading',3,*keys, *memu)
      else:
        filepath = entry[1][0][1]
        m_id = entry[1][0][0]
       
      return m_id, filepath

  def write(self, id: bytes, wc: dict[str, int],filename) -> None:
      member_score_pairs = []
      for member, score_increment in wc.items():
          member_score_pairs.extend([score_increment, member])
      keys = [COUNT,IN,'worker',id,ADDED,FNAME,filename,MYHASH2,str(self.get_timestamp()),MYHASH]
      status = self.rds.fcall('add_wc',10,*keys,*member_score_pairs)
      fil = open('latency.txt','a')
      s = str(filename + " " + str(status) + "\n")
      if(status!=0):
        fil.write(s)
      return None
    
  def is_pending(self) -> bool:
      chk = self.rds.xlen(IN) != self.rds.xlen(ADDED)
      return chk
    
  def check(self,filename):
      keys = [MYHASH,filename]
      msg = []
      status = self.rds.fcall('check',2,*keys,*msg)
      status = int(status.decode())
      if(status==1):
        print(1," status")
      return status

  def restart(self, down_time, down_port, instance_port):
      if IS_RAFT==False:
        self.rds.shutdown(nosave=True)
        os.system('sudo systemctl stop redis-server')
        time.sleep(down_time)
        os.system('sudo systemctl start redis-server')
        return
      else:
        rdscrash = Redis(host='localhost', port=down_port, password='', db=0, decode_responses=False)
        rdscrash.shutdown(nosave=True)
        time.sleep(down_time)
        os.system(f"bash configure_redis.sh {down_port}")
        time.sleep(0.5)
        try:
          self.rds.exists(f"KIO") # Check who is leader
        except Exception as e:
          e = str(e)
          e = e.split(" ")
          if e[0] == "MOVED":
            e = e[-1]
            e = e.split(":")
            e = e[-1]
            os.system(f"redis-cli -p {instance_port} RAFT.CLUSTER JOIN localhost:{e}")
            self.rds = Redis(host='localhost', port=int(e), password='', db=0, decode_responses=False)
            time.sleep(0.5)
          else:
            print("OTHER ERROR WHILE FETCHING FLAG")
        
  def writeraft(self, wc):
      for mem, incr in wc.items():
            self.rds.zincrby(COUNT, incr, mem)
    
  def get_flag(self):
      # print(self.rds.zscore(FLAGSTREAM,FLAG))
      return self.rds.zscore(FLAGSTREAM,FLAG)
    
  def update_flag(self):
      self.rds.zincrby(FLAGSTREAM,1,FLAG)

    
      

        
      
  