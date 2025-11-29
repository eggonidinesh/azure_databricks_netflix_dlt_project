# Databricks notebook source
var = dbutils.jobs.taskValues.get(
    taskKey="weekdaylookup",
    key="weekoutput",
    debugValue="SomeDebugValue"
)

# COMMAND ----------

print(var)