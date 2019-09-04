package me.liuhaidong.flink.demo

import _root_.lombok.extern.slf4j.Slf4j
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}


/**
  *
  * @author liuhaidong
  * @date 19/08/30
  *
  */
@Slf4j
object CheckPointDemo {

  /**
    * valueState状态计算
    *
    * @param args
    */
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /**
      * 首先从作业的运行环境 env.enableCheckpointing 传入 1000，意思是做 2 个 Checkpoint 的事件间隔为 1 秒。Checkpoint 做的越频繁，恢复时追数据就会相对减少，同时 Checkpoint 相应的也会有一些 IO 消耗
      */
    env.enableCheckpointing(1000)

    /**
      * Checkpoint 的 model，即设置了 Exactly_Once 语义，并且需要 Barries 对齐，这样可以保证消息不会丢失也不会重复
      */
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    /**
      * setMinPauseBetweenCheckpoints 是 2 个 Checkpoint 之间最少是要等 500ms，也就是刚做完一个 Checkpoint。比如某个 Checkpoint 做了 700ms，按照原则过 300ms 应该是做下一个 Checkpoint，因为设置了 1000ms 做一次 Checkpoint 的，但是中间的等待时间比较短，不足 500ms 了，需要多等 200ms，因此以这样的方式防止 Checkpoint 太过于频繁而导致业务处理的速度下降
      */
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)

    /**
      * setCheckpointTimeout 表示做 Checkpoint 多久超时，如果 Checkpoint 在 1min 之内尚未完成，说明 Checkpoint 超时失败。
      */
    env.getCheckpointConfig.setCheckpointTimeout(6000)

    /**
      * 表示同时有多少个 Checkpoint 在做快照，这个可以根据具体需求去做设置。
      */
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    /**
      *enableExternalizedCheckpoints 表示下 Cancel 时是否需要保留当前的 Checkpoint，默认 Checkpoint 会在整个作业 Cancel 时被删除。Checkpoint 是作业级别的保存点
      */
      env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)
    val stream: DataStream[String] = env.socketTextStream("localhost", 123)
    stream.print()

  }
}
