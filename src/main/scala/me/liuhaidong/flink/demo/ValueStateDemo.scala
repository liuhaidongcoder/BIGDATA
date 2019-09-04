package me.liuhaidong.flink.demo

import _root_.lombok.extern.slf4j.Slf4j
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListStateDescriptor, _}
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
object ValueStateDemo {

  def main(args: Array[String]) {




    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = env.socketTextStream("localhost", 123)

    stream.map(line => (line, 1)).keyBy(0).flatMap(new RichFlatMapFunction[(String, Int), (String, Int)] {
      var currentState: ValueState[Int] = null
      override def flatMap(in: (String, Int), collector: Collector[(String, Int)]): Unit = {
        val state: Int = currentState.value()
        currentState.update(state + 1)
        collector.collect((in._1,currentState.value()))
      }
      override def open(parameters: Configuration): Unit = {
        currentState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("state", classOf[Int]))

      }
    }).print()

    env.execute()

  }
}
