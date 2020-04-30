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