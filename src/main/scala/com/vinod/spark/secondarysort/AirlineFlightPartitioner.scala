package com.vinod.spark.secondarysort

import com.vinod.spark.secondarysort.AirLineSecondarySort.FlightKey
import org.apache.spark.Partitioner

/**
  * Created by 391633 on 10/27/2016.
  */
class AirlineFlightPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int =  {
    val airKey = key.asInstanceOf[FlightKey]
    airKey.airLineId.hashCode % numPartitions
  }
}
