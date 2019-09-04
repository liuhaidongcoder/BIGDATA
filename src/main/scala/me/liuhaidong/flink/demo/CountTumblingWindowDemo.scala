package me.liuhaidong.flink.demo

import lombok.extern.slf4j.Slf4j
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector


/**
  * 根据数据量开滚动窗口
  * @author liuhaidong
  * @date 19/08/30
  *
  */
@Slf4j
object CountTumblingWindowDemo {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = env.socketTextStream("localhost", 123)

    stream.countWindowAll(2).process(new ProcessAllWindowFunction[String, String, GlobalWindow] {
      @scala.throws[Exception]
      override def process(context: Context, elements: Iterable[String], out: Collector[String]): Unit = {
        var result = ""
        for (elem <- elements) {
          result += elem
        }
        out.collect(result)
      }
    }).print()

    env.execute()

  }
}
