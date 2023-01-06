# Databricks notebook source
def test(job, a, b, c):
    print(f"job: {job}, a: {a}, b: {b}, c: {c}")

# COMMAND ----------

table_list = [("a1", "b1", "c1"), ("a2", "b2", "c2"), ("a3", "b3", "c3")]
table_list2 = [{"a":"a4", "b":"b4", "c":"c4"}, {"a":"a5", "b":"b6", "c":"c6"}]

# COMMAND ----------

from threading import Thread
from queue import Queue

q = Queue()
worker_count = 3
errors = {}

def run_tasks(function, q):
    while not q.empty():
        value = q.get()
        try:
            function('concurrency', **value)
        except Exception as e:
            errors[value] = e
            msg = f"ERROR processing {value}: {str(e)}"
            log_error_message(msg)
        finally:
            q.task_done()

for table in table_list2:
    q.put(table)

for i in range(worker_count):
    t=Thread(target=run_tasks, args=(test, q))
    t.daemon = True
    t.start()

q.join()

if len(errors) == 0:
    print("All tasks completed successfully.")
elif len(errors) > 0:
    msg = f"Errors during tasks {list(errors.keys())} -> \n {str(errors)}"
    raise Exception(msg)



# COMMAND ----------


