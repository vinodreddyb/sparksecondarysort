package com.vinod.spark.secondarysort

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

/**
  * Created by 391633 on 10/27/2016.
  */
object AirLineSecondarySort {

  case class FlightKey(airLineId : String, arrivalAirportId: Int, arrivalDelay: Double) {
    override def  toString() : String = {
      s"$airLineId,$arrivalAirportId,$arrivalDelay"
    }
  }
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/winutils")
    val conf = new SparkConf().setAppName("Secondary sort ").setMaster("local[*]")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val rawDataRdd = sc.textFile("src/main/resources/160835276_T_ONTIME.csv").map(_.split(","))
    val airLineData = rawDataRdd.map (createKeyValueTuple(_))

    val sorted = airLineData
      .repartitionAndSortWithinPartitions(new AirlineFlightPartitioner(1))
      .map(data => s"${data._1}, ${data._2.mkString(",")}")


    sorted.saveAsTextFile("test")

  }

  def createKeyValueTuple(data: Array[String]) :(FlightKey,List[String]) = {
    (createKey(data),listData(data))
  }
  def createKey(data: Array[String]): FlightKey = {
    FlightKey(data(1), Try(data(8).toInt).getOrElse(0), Try(data(14).toDouble).getOrElse(0.0))
  }
  def listData(data: Array[String]): List[String] = {
    List(data(0), data(4), data(6), data(10))
  }

  object FlightKey {
    implicit def orderingByIdAirportIdDelay[A <: FlightKey] : Ordering[A] = {
      Ordering.by(fk => (fk.airLineId,fk.arrivalAirportId,fk.arrivalDelay))
    }
  }
}

