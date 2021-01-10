package bixi

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{SaveMode, SparkSession}

object Main extends App with HDFS {

  if(fs.delete(new Path(s"$uri/user/winter2020/iuri/bixi"),true))
    println("Folder Bixi deleted before Instantiate!")

  fs.mkdirs(new Path(s"$uri/user/winter2020/iuri/bixi"))

  fs.copyFromLocalFile(new Path("input/system_alerts.json"),
    new Path(s"$uri/user/winter2020/iuri/bixi/system_alerts/system_alerts.json"))
  fs.copyFromLocalFile(new Path("input/system_information.json"),
    new Path(s"$uri/user/winter2020/iuri/bixi/system_information/system_information.json"))
  fs.copyFromLocalFile(new Path("input/station_status.json"),
    new Path(s"$uri/user/winter2020/iuri/bixi/station_status/station_status.json"))
  fs.copyFromLocalFile(new Path("input/station_information.json"),
    new Path(s"$uri/user/winter2020/iuri/bixi/station_information/station_information.json"))

  val spark = SparkSession.builder().appName("Bixi Project V5").master("local[*]").getOrCreate()

  val systemAlertsFile = "/user/winter2020/iuri/bixi/system_alerts/system_alerts.json"
  val systemInformationFile = "/user/winter2020/iuri/bixi/system_information/system_information.json"
  val stationStatusFile = "/user/winter2020/iuri/bixi/station_status/station_status.json"
  val stationInformationFile = "/user/winter2020/iuri/bixi/station_information/station_information.json"

  val systemAlertsDf = spark.read.json(s"$uri$systemAlertsFile")
  val systemInformationDf = spark.read.json(s"$uri$systemInformationFile")
  val stationStatusDf = spark.read.json(s"$uri$stationStatusFile")
  val stationInformationDf = spark.read.json(s"$uri$stationInformationFile")

  systemAlertsDf.printSchema()
  systemInformationDf.printSchema()
  stationStatusDf.printSchema()
  stationInformationDf.printSchema()

  systemAlertsDf.createOrReplaceTempView("system_alerts")
  systemInformationDf.createOrReplaceTempView("system_information")
  stationStatusDf.createOrReplaceTempView("station_status")
  stationInformationDf.createOrReplaceTempView("station_information")

  val lengthSystemAlerts = spark.sql("""SELECT size(data.alerts) as Size FROM system_alerts""")
    .take(1)(0).getInt(0)
  println(lengthSystemAlerts)
  var l1 = 0
  while (l1 < lengthSystemAlerts) {
    val systemAlertsQuery = spark.sql(
      s"""
         |SELECT data.alerts[$l1].alert_id,
         |data.alerts[$l1].description,
         |data.alerts[$l1].last_updated,
         |data.alerts[$l1].summary,
         |data.alerts[$l1].type,
         |data.alerts[$l1].url
         |FROM system_alerts
         |""".stripMargin)
    systemAlertsQuery.write.mode(SaveMode.Append).csv(s"$uri/user/winter2020/iuri/bixi/system_alerts_csv")
    l1 = l1+1
  }

  val systemInformationQuery = spark.sql(
    """
         |SELECT data.email as Email,
         |data.language as Language,
         |data.license_url,
         |data.name as Name,
         |data.operator,
         |data.phone_number,
         |data.purchase_url,
         |data.short_name,
         |data.start_date,
         |data.system_id,
         |data.timezone,
         |data.url
         |FROM system_information
         |""".stripMargin)
    systemInformationQuery.write.mode(SaveMode.Append).csv(s"$uri/user/winter2020/iuri/bixi/system_information_csv")

  val lengthStationStatus = spark.sql("""SELECT size(data.stations) as Size FROM station_status""")
    .take(1)(0).getInt(0)
  println(lengthStationStatus)
  var l3 = 0
  while (l3 < lengthStationStatus) {
  val stationStatusQuery = spark.sql(
    s"""
      |SELECT data.stations[$l3].station_id as station_id,
      |data.stations[$l3].is_charging,
      |data.stations[$l3].is_installed,
      |data.stations[$l3].is_renting,
      |data.stations[$l3].is_returning,
      |data.stations[$l3].last_reported,
      |data.stations[$l3].num_bikes_available as Num_Bikes_Avail,
      |data.stations[$l3].num_bikes_disabled as Num_Bikes_Dis,
      |data.stations[$l3].num_docks_available as Num_Docks_Avail,
      |data.stations[$l3].num_docks_disabled as Num_Docks_Dis,
      |data.stations[$l3].num_ebikes_available as Num_EBikes_Avail,
      |data.stations[$l3].eightd_has_available_keys as Has_Key
      |FROM station_status
      |""".stripMargin)
    stationStatusQuery.write.mode(SaveMode.Append).csv(s"$uri/user/winter2020/iuri/bixi/station_status_csv")
    l3 = l3+1
  }

  val lengthSystemInformation = spark.sql("""SELECT size(data.stations) as Size FROM station_information""")
    .take(1)(0).getInt(0)
  println(lengthSystemInformation)
  var l4 = 0
  while (l4 < lengthSystemInformation) {
    val stationInformationQuery2 = spark.sql(
      s"""
         |SELECT data.stations[$l4].external_id as external_id,
         |data.stations[$l4].capacity as Capacity,
         |data.stations[$l4].eightd_has_key_dispenser as e_h_k_d,
         |data.stations[$l4].electric_bike_surcharge_waiver as e_b_s_w,
         |data.stations[$l4].has_kiosk as Has_Kiosk,
         |data.stations[$l4].is_charging as Is_Charging,
         |data.stations[$l4].lat as Lat,
         |data.stations[$l4].lat as Lon,
         |data.stations[$l4].name as Name,
         |data.stations[$l4].rental_methods[0] as Elem,
         |data.stations[$l4].short_name as S_N,
         |data.stations[$l4].station_id as station_id
         |FROM station_information
         |""".stripMargin)
    stationInformationQuery2.write.mode(SaveMode.Append).csv(s"$uri/user/winter2020/iuri/bixi/station_information_csv")
    l4 = l4+1
  }

  stmt.execute("SET hive.exec.dynamic.partition.mode = nonstrict")
  stmt.executeUpdate("""CREATE DATABASE IF NOT EXISTS winter2020_iuri""".stripMargin)
  stmt.executeUpdate("""DROP TABLE ext_system_alerts_v5""".stripMargin)
  stmt.executeUpdate("""DROP TABLE ext_system_information_v5""".stripMargin)
  stmt.executeUpdate("""DROP TABLE ext_station_status_v5""".stripMargin)
  stmt.executeUpdate("""DROP TABLE ext_station_information_v5""".stripMargin)

  stmt.executeUpdate(
    """CREATE EXTERNAL TABLE ext_system_alerts_v5 (
      |alert_id         STRING,
      |description      STRING,
      |last_updated     DOUBLE,
      |summary          STRING,
      |type             STRING,
      |url              STRING
      |)
      |row format DELIMITED
      |fields TERMINATED BY ','
      |stored as textfile
      |LOCATION '/user/winter2020/iuri/bixi/system_alerts_csv'
      |""".stripMargin
  )

  stmt.executeUpdate(
    """CREATE EXTERNAL TABLE ext_system_information_v5 (
      |email         STRING,
      |language      STRING,
      |license_url   STRING,
      |name          STRING,
      |operator      STRING,
      |phone_number  STRING,
      |purchase_url  STRING,
      |short_name    STRING,
      |start_date    STRING,
      |system_id     STRING,
      |timezone      STRING,
      |url           STRING
      |)
      |row format DELIMITED
      |fields TERMINATED BY ','
      |stored as textfile
      |LOCATION '/user/winter2020/iuri/bixi/system_information_csv'
      |""".stripMargin
  )

  stmt.executeUpdate(
    """CREATE EXTERNAL TABLE ext_station_status_v5 (
      |station_id           INT,
      |is_charging          STRING,
      |is_installed         DOUBLE,
      |is_renting           DOUBLE,
      |is_returning         DOUBLE,
      |last_reported        DOUBLE,
      |num_bikes_available  DOUBLE,
      |num_bikes_disabled   DOUBLE,
      |num_docks_available  DOUBLE,
      |num_docks_disabled   DOUBLE,
      |num_ebikes_available DOUBLE,
      |eigthd_has_available_keys STRING
      |)
      |row format DELIMITED
      |fields TERMINATED BY ','
      |stored as textfile
      |LOCATION '/user/winter2020/iuri/bixi/station_status_csv'
      |""".stripMargin
  )

  stmt.executeUpdate(
    """CREATE EXTERNAL TABLE ext_station_information_v5 (
      |External_id         STRING,
      |Capacity            INT,
      |Eightd_H_K_D        STRING,
      |e_b_s_w             STRING,
      |Has_Kiosk           STRING,
      |Is_Charging         STRING,
      |Lat                 DOUBLE,
      |Lon                 DOUBLE,
      |Name                STRING,
      |Elem                STRING,
      |S_N                 STRING,
      |station_id          INT
      |)
      |row format DELIMITED
      |fields TERMINATED BY ','
      |stored as textfile
      |LOCATION '/user/winter2020/iuri/bixi/station_information_csv'
      |""".stripMargin
  )

}

