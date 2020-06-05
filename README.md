# YAO-scheduler


## API

**GetHeartCounter**

```
?action=get_counter
```

**GetJobTaskStatusJHL**

```
?action=jhl_job_status&job=
```

**GetBindings**

GPU is occupied by which job(s)

```
?action=get_bindings
```

**EnableSchedule**
```
?action=debug_enable
```

**DisableSchedule**
```
?action=debug_disable
```

**UpdateMaxParallelism**
```
?action=debug_update_parallelism&parallelism=5
```


**getAllPredicts**
```
?action=debug_get_predicts
```


**getAllGPUUtils**
```
?action=debug_get_gpu_utils
```


**SetShareRatio**
```
?action=debug_update_enable_share_ratio&ratio=0.75
```


**SetPreScheduleRatio**
```
?action=debug_update_enable_pre_schedule_ratio&ratio=0.95
```

**FeedDLData**
```
?action=debug_optimizer_feed_dl&job=lstm&seq=1&value=2
```

**TrainDL**
```
?action=debug_optimizer_train_dl&job=lstm
```

**PredictDL**
```
?action=debug_get_predict_dl&job=lstm&seq=1
```

**UpdateAllocateStrategy**
```
?action=allocator_update_strategy&strategy=bestfit
```

**SchedulerDump**
```
?action=debug_scheduler_dump
```

**DescribeJob**
```
?action=debug_optimizer_describe_job&job=
```