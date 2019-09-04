package me.liuhaidong.flink.demo

import _root_.lombok.extern.slf4j.Slf4j
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListStateDescriptor, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector


/**
  * MapState
  * @author liuhaidong
  * @date 19/08/30
  *
  */
@Slf4j
object MapStateDemo {


  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    val stream: DataStream[String] = env.socketTextStream("localhost", 123)

    stream.map(line => (line, 1)).keyBy(0).flatMap(new RichFlatMapFunction[(String, Int), (String, Int)] {
      var currentMapState: MapState[String, Int] = null

      override def flatMap(in: (String, Int), collector: Collector[(String, Int)]): Unit = {
        if (currentMapState == null) {
          currentMapState.put(in._1, 1)
        } else {
          val count = currentMapState.get(in._1)
          currentMapState.put(in._1, count + 100)
        }
        collector.collect((in._1, currentMapState.get(in._1)))
      }

      override def open(parameters: Configuration): Unit = {
        currentMapState = getRuntimeContext.getMapState(new MapStateDescriptor[String, Int]("mapState", classOf[String], classOf[Int]))
      }
    }).print()

    env.execute()

  }
}
