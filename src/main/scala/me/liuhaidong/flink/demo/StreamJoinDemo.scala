package me.liuhaidong.flink.demo

import _root_.lombok.extern.slf4j.Slf4j
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time


/**
  * 流和流之间的join
  * @author liuhaidong
  * @date 19/08/30
  *
  */
@Slf4j
object StreamJoinDemo {


  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    val streamA: DataStream[String] = env.socketTextStream("localhost", 123)
    val streamB: DataStream[String] = env.socketTextStream("localhost", 1234)
    val keydStreamA = streamA.map(line => (line,1)).keyBy(0)
    val keydStreamB = streamB.map(line => (line,1)).keyBy(0)

    keydStreamA.intervalJoin(keydStreamB).between(Time.minutes(0),Time.minutes(5))

    env.execute()
  }
}
