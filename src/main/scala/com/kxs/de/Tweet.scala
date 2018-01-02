package com.kxs.de

import twitter4j.{Place, Status}

case class Tweet(userName: String, country: String, language: String, hastags: String, text: String, retweetCount: Int, createdAt: String)

object Tweet {
  def apply(status: Status): Tweet = {
    val userName: String = status.getUser.getName
    val place: Option[Place] = Option(status.getPlace)
    val country: String = place.map(_.getCountry).getOrElse("")
    val language: String = status.getLang
    val hastags: String = status.getHashtagEntities.map(e => e.getText).mkString(",")
    val text: String = status.getText
    val retweetCount: Int = status.getRetweetCount
    import java.text.SimpleDateFormat
    val formatter = new SimpleDateFormat("yyyyMMddHHmmss")
    val createdAt:String = formatter.format(status.getCreatedAt)

    Tweet(userName, country, language, hastags, text, retweetCount, createdAt)
  }
}