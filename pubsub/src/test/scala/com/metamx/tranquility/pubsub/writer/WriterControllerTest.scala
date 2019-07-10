package com.metamx.tranquility.pubsub.writer

import java.util.Properties

import com.fasterxml.jackson.databind.ObjectMapper
import com.metamx.common.scala.Logging
import com.metamx.tranquility.config.DataSourceConfig
import com.metamx.tranquility.pubsub.model
import com.metamx.tranquility.pubsub.model.PropertiesBasedPubSubConfig
import com.metamx.tranquility.test.common.{CuratorRequiringSuite, DruidIntegrationSuite}
import io.druid.query.aggregation.{AggregatorFactory, CountAggregatorFactory}
import io.druid.segment.indexing.{DataSchema, RealtimeIOConfig, RealtimeTuningConfig}
import io.druid.segment.realtime.{FireDepartment, FireDepartmentMetrics}
import io.druid.segment.realtime.firehose.LocalFirehoseFactory
import io.druid.segment.realtime.plumber.{Plumber, PlumberSchool}
import org.scalatest.{BeforeAndAfter, FunSuite, ShouldMatchers}
import org.skife.config.ConfigurationObjectFactory

//class WriterControllerTest extends FunSuite with DruidIntegrationSuite with BeforeAndAfter with CuratorRequiringSuite with ShouldMatchers with Logging {
//  val writerController: WriterController
//
//  before {
//    val props = new Properties()
//    val propsTwitter = new Properties(props)
//    propsTwitter.setProperty("topicPattern", "twitter")
//
//    val propsTest = new Properties(props)
//    propsTest.setProperty("topicPattern", "test[0-9]")
//    propsTest.setProperty("useTopicAsDataSource", "true")
//
//    val fdTwitter = new FireDepartment(
//      new DataSchema("twitter", null, Array(new CountAggregatorFactory("cnt")), null, new ObjectMapper()),
//      new RealtimeIOConfig(
//        new LocalFirehoseFactory(null, null, null),
//        new PlumberSchool {
//          override def findPlumber(schema: DataSchema, config: RealtimeTuningConfig, metrics: FireDepartmentMetrics): Plumber = null
//        }, null
//      ), null
//    )
//
//    val fdTest = new FireDepartment(
//      new DataSchema("test[0-9]", null, Array(new CountAggregatorFactory("cnt")), null, new ObjectMapper()),
//      new RealtimeIOConfig(
//        new LocalFirehoseFactory(null, null, null),
//        new PlumberSchool {
//          override def findPlumber(schema: DataSchema, config: RealtimeTuningConfig, metrics: FireDepartmentMetrics): Plumber = null
//        }, null
//      ), null
//    )
//
//
////    val dataSourceConfigs = Map(
////      "twitter" -> new DataSourceConfig[PropertiesBasedPubSubConfig]("twitter", new ConfigurationObjectFactory(propsTwitter).build(), )
////    )
////    writerController = new WriterController()
//  }
//
////  test("Get Writer Controller") {
////    withDruidStack {
////
////    }
////  }
//
//}
