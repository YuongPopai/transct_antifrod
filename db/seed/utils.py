
from random import randint, choice, random
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from uuid import uuid4


CHANNELS = ["online", "mobile", "atm", "branch", "card"]


def random_amount():
  base = randint(1, 10000)
  if random() < 0.02:
    base *= randint(5, 30) 
  return round(base / 1.0, 2)


def random_dates(now, months_back=12):
  start = now - relativedelta(months=months_back)
  delta = now - start
  initiated = start + timedelta(seconds=randint(0, int(delta.total_seconds())))
  settled = initiated + timedelta(minutes=randint(1, 120))
  return initiated, settled


def rand_channel():
  return choice(CHANNELS)