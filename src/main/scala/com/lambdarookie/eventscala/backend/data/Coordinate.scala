package com.lambdarookie.eventscala.backend.data

import scala.math._

/**
  * @param latitude Latitude
  * @param longitude Longitude
  * @param altitude Altitude in meters
  */
case class Coordinate(latitude: Double, longitude: Double, altitude: Double) {

  /**
    * The Java code from [[http://stackoverflow.com/a/16794680]] converted to Scala
    *
    * Calculate distance between two points in latitude and longitude taking
    * into account height difference. If you are not interested in height
    * difference pass 0.0. Uses Haversine method as its base.
    * @param coordinate Coordinate to calculate to
    * @return Distance in meters
    */
  def calculateDistanceTo(coordinate: Coordinate): Int = {
    val R = 6371 // Radius of the earth
    val latDistance = toRadians(latitude - coordinate.latitude)
    val lonDistance = toRadians(longitude -coordinate.longitude)
    val a = sin(latDistance / 2) * sin(latDistance / 2) +
      cos(toRadians(latitude)) * cos(toRadians(coordinate.latitude))* sin(lonDistance / 2) * sin(lonDistance / 2)
    val c = 2 * atan2(sqrt(a), sqrt(1-a))
    val distance = R * c * 1000 // convert to meters
    val height = altitude - coordinate.altitude
    sqrt(pow(distance, 2) + pow(height, 2)).toInt
  }
}
