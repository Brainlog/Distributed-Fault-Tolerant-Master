from __future__ import annotations

import logging
import os
import signal
import sys
from abc import abstractmethod, ABC
from threading import current_thread
from typing import Any, Final
from mrds import MyRedis
import time

class Saver():
  def __init__(self, **kwargs: Any):
    self.name = "saver"
    self.pid = -1
    
  def create_and_run(self, **kwargs: Any) -> None:
    pid = os.fork()
    assert pid >= 0
    if pid == 0:
      # Child worker process
      self.pid = os.getpid()
      self.name = f"saver-{self.pid}"
      thread = current_thread()
      thread.name = self.name
      logging.info(f"Starting")
      self.run(**kwargs)
      sys.exit()
    else:
      self.pid = pid
      self.name = f"saver-{pid}"

  def run(self, **kwargs: Any) -> None:
      rds: MyRedis = kwargs['rds']
      loki = True
      while loki:
        try:
          rds.rds.bgsave()
          time.sleep(3)
        except Exception as e:
          continue
        
        
  def kill(self) -> None:
    logging.info(f"Killing {self.name}")
    os.kill(self.pid, signal.SIGKILL)