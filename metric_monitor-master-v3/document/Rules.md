#**缩容/扩容**

---
#说明
**联通目前只支持缩容, 然后恢复到默认值. 不支持做扩容**

**缩容 -> 缩容到人为定义的值**

**扩容 -> 恢复到正常用户配置的值**

---

#缩容

###Inceptor Executor缩容条件

**1. Inceptor当前Running Jobs/Active Tasks/Pending Tasks数量为0**

**2. Inceptor过去一段时间(Config.LAST_DURATION_TIME)没有任何Job提交**


###Inceptor Executor缩容规则

**1. Executor的副本个数必须大于Config.EXECUTOR_MIN_REPLICA**

**2. Executor的副本个数修改为Config.EXECUTOR_MIN_REPLICA**

---

#扩容

###Inceptor Executor扩容条件

**1. Inceptor瞬时Active Tasks数量大于Config.EXECUTOR_MAX_ACTIVE_TASK**

**2. Inceptor瞬时Pending Tasks数量大于Config.EXECUTOR_MAX_PENDING_TASK**

**3. Inceptor瞬时Running Jobs数量大于Config.EXECUTOR_MAX_ACTIVE_JOB**

**4. Inceptor过去一段时间(Config.LAST_DURATION_TIME) Job 提交数大于 Config.EXECUTOR_DURATION_MAX_SUBMIT_JOB**

**5. Inceptor过去一段时间(Config.LAST_DURATION_TIME) Stages 提交数大于 Config.EXECUTOR_DURATION_MAX_SUBMIT_STAGE**

**6. Executor的副本数小于Config.EXECUTOR_MAX_REPLICA**

---

###Inceptor Executor扩容规则

**1. Executor的副本个数必须小于Config.EXECUTOR_MAX_REPLICA**

**2. 物理节点(没有调度对应Executor)的(内存/Cpu)没有超过50%的数量大于Config.EXECUTOR_EVERY_INCREASE_OR_DECREASE_CAPACITY**

**3. Executor个数修改为Config.EXECUTOR_MAX_REPLICA**

---

#FAQ

**1. 是否考虑datanode和executor对应的关系,数据本地化问题**

**2. 缺少exporter-inceptor, 由于DBAService/Inceptor没有暴露metric**

**3. Inceptor具体业务指标判断扩容／缩容还需要找inceptor和看业务需求**

**4. 部分Executor的pod是不正常状态,这个怎么算**

**5. Pending Stage有多少, 每个Pending Stage持续多少**

**6. Pending Stage有多少, 每个Pending Stage持续多少**