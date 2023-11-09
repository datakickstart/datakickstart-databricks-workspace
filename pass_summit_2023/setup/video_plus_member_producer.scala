// Databricks notebook source
import kafkashaded.org.apache.kafka.clients.producer._

// COMMAND ----------

import java.sql.Timestamp
import java.time.Instant
import java.util.Properties
import java.time.temporal.ChronoUnit


import scala.util.Random
import kafkashaded.org.apache.kafka.clients.producer._

val bootstrapServers = dbutils.secrets.get(scope = "demo", key = "confluent-cloud-brokers")
val mode: String = "confluent" // 'eventhubs' // 'local'

val props = new Properties()
props.put("key.serializer", "kafkashaded.org.apache.kafka.common.serialization.StringSerializer")
props.put("value.serializer", "kafkashaded.org.apache.kafka.common.serialization.StringSerializer")

if (mode.toLowerCase == "local") {
  println("Running producer in local mode")
  props.put("bootstrap.servers", "localhost:9092")
} else if (mode.toLowerCase == "confluent") {
  println("Running producer in Confluent Cloud mode")
  // If using Confluent
  val kafkaAPIKey = dbutils.secrets.get(scope = "demo", key = "confluent-cloud-user")
  val kafkaAPISecret = dbutils.secrets.get(scope = "demo", key = "confluent-cloud-password")
  props.put("bootstrap.servers", bootstrapServers)
  props.put("security.protocol", "SASL_SSL")
  props.put("sasl.jaas.config", s"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule  required username='$kafkaAPIKey'   password='$kafkaAPISecret';")
  props.put("sasl.mechanism", "PLAIN")
  // Required for correctness in Apache Kafka clients prior to 2.6
  props.put("client.dns.lookup", "use_all_dns_ips")
  // Best practice for Kafka producer to prevent data loss
  props.put("acks", "all")
} else if (mode.toLowerCase() == "eventhubs") {
  println("Running producer in Event Hubs mode")
}

def produceRecord(producer: KafkaProducer[String, String], record: ProducerRecord[String, String]) = {
  val metadata = producer.send(record)
  printf(s"sent to topic %s: record(key=%s value=%s) " +
    "meta(partition=%d, offset=%d)\n",
    record.topic(), record.key(), record.value(),
    metadata.get().partition(),
    metadata.get().offset()
  )
}

// COMMAND ----------

val timestamp = () => Timestamp.from(Instant.now)
val startInstant = Instant.now
val startDate1 = Timestamp.from(startInstant.minus(1, ChronoUnit.DAYS ))
val endDate1 = Timestamp.from(startInstant.plus(30, ChronoUnit.DAYS ))

case class MembershipRecord(membershipId: String, user: String, planId: String, startDate: Timestamp, endDate: Timestamp, updatedAt: Timestamp, eventTimestamp: Timestamp)


def writeUsageAndMembershipToKafka(): Unit = {
  val usageTopic = "usage"
  val membershipTopic = "membership"

  val producer = new KafkaProducer[String, String](props)

  val randomPass = () => { if (Random.nextInt(2) == 1) true else false }
  val randomUser = () => { Random.nextInt(21) }
  val randomDuration= () => { Random.nextInt(360) }
  val timestamp = () => Timestamp.from(Instant.now)
  val startInstant = Instant.now

  val numRecords = 10000
  val sleepMilliseconds = 500
  val pauseThreshold = 1  // at which record should pause start being enforced, 0 means all records

  try {
    for (i <- 0 to numRecords) {
      val usageInfo = s"""{"usageId": $i, "user": "user${randomUser()}", "completed": ${randomPass()}, "durationSeconds": ${randomDuration()}, "eventTimestamp": "${timestamp()}"}"""
      val record1 = new ProducerRecord[String, String](usageTopic, i.toString, usageInfo)
      println(usageInfo)
      produceRecord(producer, record1)

      if (i % 3 == 0) {
        val membershipUser = randomUser()
        val plan = s"plan_${membershipUser % 4}"
        val membershipInfo = s"""{"membershipId": "${membershipUser.toString}", "user": "user$membershipUser", "planId": "$plan", "startDate": "${Timestamp.from(startInstant.minus(membershipUser, ChronoUnit.MINUTES ))}", "endDate": "${Timestamp.from(startInstant.plus(30, ChronoUnit.DAYS ))}", "updatedAt":"${timestamp()}", "eventTimestamp":"${timestamp()}"}"""
        val record2 = new ProducerRecord[String, String](membershipTopic, membershipUser.toString, membershipInfo)
        println(membershipInfo)
        produceRecord(producer, record2)
      }

      if (i > pauseThreshold /*55*/) {
        Thread.sleep(sleepMilliseconds)
      }
    }
  }catch{
    case e:Exception => e.printStackTrace()
  }finally {
    producer.close()
  }
}


// COMMAND ----------

writeUsageAndMembershipToKafka()
