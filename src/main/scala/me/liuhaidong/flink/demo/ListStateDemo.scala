package me.liuhaidong.flink.demo

import _root_.lombok.extern.slf4j.Slf4j
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector


/**
  *
  * @author liuhaidong
  * @date 19/08/30
  *
  */
@Slf4j
object ListStateDemo {


  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    val stream: DataStream[String] = env.socketTextStream("localhost", 123)
    //keval keydSteam = stream.

    stream.map(line => (line, 1)).keyBy(0).flatMap(new RichFlatMapFunction[(String, Int), (String, Int)] {
      var currentListState: ListState[String] = null
      //var reducingState:ReducingState[String] = null

      override def flatMap(in: (String, Int), collector: Collector[(String, Int)]): Unit = {
        currentListState.add(in._1)
      }

      override def open(parameters: Configuration): Unit = {
        currentListState = getRuntimeContext.getListState(new ListStateDescriptor[String]("listState", classOf[String]))
      }
    }).print()

    env.execute()
  }
}
