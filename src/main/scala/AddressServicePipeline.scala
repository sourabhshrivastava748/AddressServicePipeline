//import com.google.common.base.CharMatcher
import com.google.i18n.phonenumbers.PhoneNumberUtil
import com.google.i18n.phonenumbers.PhoneNumberUtil.{PhoneNumberFormat, PhoneNumberType}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._

import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import javax.mail.internet.InternetAddress
import java.sql.DriverManager
import java.sql.Connection
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoClient
import com.mongodb.MongoClientSettings
import com.mongodb.ConnectionString
import com.mongodb.ServerAddress
import com.mongodb.MongoCredential
import com.mongodb.MongoClientOptions
import com.mongodb.MongoException
import com.mongodb.client.MongoDatabase
import com.mongodb.client.MongoCollection
import com.mongodb.client.DistinctIterable
import com.mongodb.client.MongoCursor
import org.bson.Document
import com.mongodb.client.model.Filters
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.codec.binary.Base64
import org.apache.spark.sql.functions.udf
import org.springframework.http.HttpStatus

import java.io.{FileNotFoundException, IOException, Serializable}
import java.util
import scala.collection.Seq
import scala.io.Source
import com.bettercloud.vault.response.LogicalResponse
import com.bettercloud.vault.VaultConfig
import com.bettercloud.vault.{Vault, VaultException}
import com.unifier.univault.utils.EncryptionContext
import com.unifier.univault.exception.{UniVaultException, VaultKeyNotFoundException}

import java.util.regex.Pattern
import java.sql.{Connection, DriverManager}
import com.bettercloud.vault.{Vault, VaultException}
import com.bettercloud.vault.response.LogicalResponse
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.unifier.core.utils.DateUtils.DATE_MONTH_YEAR_FORMAT
import com.unifier.core.utils.{CryptoUtils, DateUtils, StringUtils}
import com.unifier.univault.cache.{VaultCredentialsCache, VaultPropertiesCache}
import com.unifier.univault.encryption.dto.EncryptionKey
import com.unifier.univault.exception.{UniVaultException, VaultKeyNotFoundException}
import com.unifier.univault.vault.UniVaultConfig
import org.sparkproject.guava.base.CharMatcher
import org.springframework.http.HttpStatus

import java.text.SimpleDateFormat
import java.util
import java.util.{Arrays, Date, Properties}
import java.security.InvalidKeyException
import java.security.spec.KeySpec
import javax.crypto.spec.{DESedeKeySpec, SecretKeySpec}
//import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64

import java.util.concurrent.TimeUnit
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


case class UniwareShippingPackage(name: String, mobile: String, notification_mobile: String, email: String, notification_email: String, address_line1: String, address_line2: String, city: String, district: String, state_code: String, country_code: String, pincode: String, uniware_sp_created: String, uniware_sp_updated: String, tenant_code: String, facility_code: String, shipping_package_code: String, channel_source_code: String, shipping_package_uc_status: String, sale_order_code: String, sale_order_uc_status: String, sale_order_turbo_status: String) extends Serializable
case class ExplodedUniwareShippingPackage(enabled: Boolean, turbo_created: String, turbo_updated: String, turbo_mobile: String, turbo_email: String, uniwareShippingPackage: UniwareShippingPackage) extends Serializable
case class VaultKey(tenantCode: String, keyAlias: String, vaultKey: String) extends Serializable

object AddressServicePipeline {

  def main(args: Array[String]): Unit = {

    def createSparkSession(): SparkSession = {
      val startTime = System.nanoTime()
      val sparkSession = SparkSession.builder()
        .appName("UCMobileEmailAddressPipeline")
        .master("yarn")
        .config("spark.dynamicAllocation.enabled", false)
        //parse only required columns  - columnPruning (Column pruning can read only necessary columns from parquet column)
        .config("spark.files.useFetchCache", false)
        .config("spark.sql.csv.parser.columnPruning.enabled", false)
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=IST")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=IST")
        .config("spark.scheduler.allocation.file", "file:///spark/fair.xml")
        //Configures the number of partitions to use when shuffling data for joins or aggregations.
        .config("spark.sql.shuffle.partitions", 1000)
        .config("spark.ui.retainedJobs", 5)
        .config("spark.ui.retainedStages", 5)
        .config("spark.ui.retainedTasks", 10).getOrCreate()

      sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", "production-fair")
      sparkSession.sparkContext.setLocalProperty("spark.scheduler.allocation.file", "file:///spark/fair.xml")
      sparkSession.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
      //    sparkSession.sparkContext.setLogLevel("DEBUG")
      print("init Time: ", System.nanoTime() - startTime)
      return sparkSession
    }

    val invalid_phone_type_NotMobile_FIXED_LINE_OR_MOBILE = "NotMobile-FIXED_LINE_OR_MOBILE"
    val invalid_phone_type_NotMobile_Not_a_Phone_number = "The string supplied did not seem to be a phone number."
    val pincode_regex = "^[1-9]{1}[0-9]{2}\\s{0,1}[0-9]{3}$"
    val compiledValidPincodePattern = Pattern.compile(pincode_regex)
    val tentConsecutiveDigitsPattern = "([6-9][0-9]{9})".r
    val tentConsecutiveDigitsInBetWeenPattern = "(.*)([6-9][0-9]{9})(.*)".r
    val commaSepTentConsecutiveDigitsPattern = "([6-9][0-9]{9})(,)([6-9][0-9]{9})".r
    val commaSepTentConsecutiveDigitsPatternInBetween = "(.*)([6-9][0-9]{9})(,)([6-9][0-9]{9})(.*)".r
    val sparkSession: SparkSession = createSparkSession()
    val emptyStringBroadcast: Broadcast[String] = sparkSession.sparkContext.broadcast("")
    val countryBroadcastString: Broadcast[String] = sparkSession.sparkContext.broadcast("IN")
    val hyphenStringBroadcast: Broadcast[String] = sparkSession.sparkContext.broadcast("-")
    val slashNBroadcastString: Broadcast[String] = sparkSession.sparkContext.broadcast("\\N")
    val zeroBroadcastString: Broadcast[String] = sparkSession.sparkContext.broadcast("0")
    val plusNineOneBroadcastString: Broadcast[String] = sparkSession.sparkContext.broadcast("+91")
    val nineOneBroadcastString: Broadcast[String] = sparkSession.sparkContext.broadcast("91")
    val oneBroadcastInt: Broadcast[Int] = sparkSession.sparkContext.broadcast(1)
    val twoBroadcastInt: Broadcast[Int] = sparkSession.sparkContext.broadcast(2)
    val threeBroadcastInt: Broadcast[Int] = sparkSession.sparkContext.broadcast(3)
    val tenBroadcastInt: Broadcast[Int] = sparkSession.sparkContext.broadcast(10)
    val elevenBroadcastInt: Broadcast[Int] = sparkSession.sparkContext.broadcast(11)
    val twelveBroadcastInt: Broadcast[Int] = sparkSession.sparkContext.broadcast(12)
    val thirteenBroadcastInt: Broadcast[Int] = sparkSession.sparkContext.broadcast(13)
    val commaBroadcastString: Broadcast[String] = sparkSession.sparkContext.broadcast(",")
    val nanBroadcastString: Broadcast[String] = sparkSession.sparkContext.broadcast("nan")
    val current_date_timeBroadcast: Broadcast[String] = sparkSession.sparkContext.broadcast(java.time.LocalDateTime.now().toString())
    val startendDateArray: Array[String] = sparkSession.sparkContext.getConf.get("spark.driver.args").split(",")
    val excludeServers: Set[String] = Set("db.myntra-in.unicommerce.infra", "db.lenskart-in.unicommerce.infra", "db.lenskartmp-in.unicommerce.infra", "db.ril-in.unicommerce.infra")
    // yyyy-mm-dd hh:mm:ss
//    val fromInclusiveDate = "'2023-07-01 00:01:00'"
//    val tillExclusiveDate = "'2023-07-01 00:02:00'"
    val fromInclusiveDate = "'" + startendDateArray(0) + "'"
    val tillExclusiveDate = "'" + startendDateArray(1) + "'"
    val terminatedDBServer = "db.terminated-tenants-in.unicommerce.infra"

    def formatPhone(phone: String): String = {
      val phoneNumberUtil = PhoneNumberUtil.getInstance()
      return phoneNumberUtil.format(phoneNumberUtil.parse(phone, countryBroadcastString.value), PhoneNumberFormat.E164)
    }

    def removeSpace(str: String): String = {
      if (str == null) {
        return emptyStringBroadcast.value
      }
      return str.filterNot(_.isWhitespace).filterNot(_.isSpaceChar)
    }

    def tenDigitsMatch(phone: String): Option[String] = {
      try {
        val tentConsecutiveDigitsPattern(x) = removeSpace(phone)
        return Some(x)
      } catch {
        case e: Throwable => None
      }
      return None
    }

    def tenDigitsMatchInBetween(p: String): Option[String] = {
      try {
        val tentConsecutiveDigitsInBetWeenPattern(any1, mobile, any2) = removeSpace(p)
        return Some(mobile)
      } catch {
        case e: Throwable => None
      }
      return None
    }

    def no_recover(p: String): List[String] = {
      return scala.collection.immutable.List.empty
    }

    def checkForTenDigits(input: String): Option[String] = {
      var recoveredPhone: Option[String] = tenDigitsMatch(input)
      if (!recoveredPhone.isDefined) {
        recoveredPhone = tenDigitsMatchInBetween(input)
      }
      return recoveredPhone
    }

    def checkForTenDigitsWrapper(input: String): Option[String] = {
      val digitCount = input.count(_.isDigit)
      var recoveredPhone: Option[String] = None
      if (digitCount == 10) {
        recoveredPhone = checkForTenDigits(input)
      }
      return recoveredPhone
    }

    def removeHyphen(phone: String): String = {
      return phone.replaceAll(hyphenStringBroadcast.value, emptyStringBroadcast.value)
    }

    def isValidPinCodeAsPerRegex(pincode: String, compiledValidPincodePattern: Pattern): Boolean = {
      return compiledValidPincodePattern.matcher(pincode).matches()
    }

    def isEmpty(string: String): Boolean = {
      return string == null || string.equals(slashNBroadcastString.value) || string.isEmpty() || removeSpace(string).isEmpty()
    }

    def mobileInputPreprocessing(phone: String): Option[String] = {
      var phone_processed: Option[String] = Some(phone)
      val digitCount = phone.count(_.isDigit)
      if (phone.startsWith(zeroBroadcastString.value) && phone.length() == elevenBroadcastInt.value && digitCount == elevenBroadcastInt.value) {
        phone_processed = Some(phone.substring(oneBroadcastInt.value))
      } else if (phone.startsWith(plusNineOneBroadcastString.value) && phone.length() == thirteenBroadcastInt.value && digitCount == twelveBroadcastInt.value) {
        phone_processed = Some(phone.substring(threeBroadcastInt.value))
      } else if (phone.startsWith(nineOneBroadcastString.value)
        &&
        ((phone.length() == twelveBroadcastInt.value && digitCount == twelveBroadcastInt.value) || (phone.length() == tenBroadcastInt.value && digitCount == tenBroadcastInt.value))) {
        phone_processed = Some(phone)
        if (digitCount == twelveBroadcastInt.value) {
          phone_processed = Some(phone.substring(twoBroadcastInt.value))
        }
      }
      return phone_processed
    }

    def mobileInputPreprocessingWrapper(phone: String): List[String] = {
      var input: String = removeHyphen(removeSpace(phone))
      var validMobileList: ListBuffer[String] = ListBuffer()
      if (input.contains(commaBroadcastString.value)) {
        for (str <- input.split(commaBroadcastString.value)) {
          val phone_processed = mobileInputPreprocessing(str)
          if (phone_processed.isDefined) {
            validMobileList.append(phone_processed.get)
          }
        }
      } else {
        val phone_processed = mobileInputPreprocessing(input)
        if (phone_processed.isDefined) {
          validMobileList.append(phone_processed.get)
        }
      }
      return validMobileList.toList
    }
    // return empty list if unable to find valid mobiles
    def recover_FIXED_LINE_OR_MOBILE_Pattern(phone: String): List[String] = {
      val inputList: List[String] = mobileInputPreprocessingWrapper(phone)
      var validMobileList: ListBuffer[String] = ListBuffer()
      for (str <- inputList) {
        val recoveredPhone: Option[String] = checkForTenDigitsWrapper(str)
        if (recoveredPhone.isDefined) {
          validMobileList.append(formatPhone(recoveredPhone.get))
        }
      }
      return validMobileList.toList
    }

    def jdbcURLForUniwareServerName(serverName: String, databaseName: String): String = {
      "jdbc:mysql://" + serverName + ":3306/" + databaseName + "?useSSL=false&useServerPrepStmts=false&rewriteBatchedStatements=true&enabledTLSProtocols=TLSv1.3"
    }

    def readandBroadcastPincodes(): Broadcast[Set[String]] = {
      val start = System.nanoTime()
      val df: DataFrame = sparkSession.read.format("csv").option("header", "true").load("hdfs://10.0.5.17:9000/pincodes/pincode.csv")
      import sparkSession.implicits._
      val pincodes: Set[String] = df.select("Pincode").as[String].collect.toSet
      val pincodeBroadcast: Broadcast[Set[String]] = sparkSession.sparkContext.broadcast(pincodes)
      val end = System.nanoTime()
      val difference = end - start
      val readandBroadcastPincodes = "readandBroadcastPincode-Time-Taken-hrs %s Time-Taken-min %s Time-Taken-sec %s"
      //  val readandBroadcastPincodes_str = readandBroadcastPincodes.format(TimeUnit.NANOSECONDS.toHours(difference), (TimeUnit.NANOSECONDS.toMinutes(difference) - TimeUnit.HOURS.toMinutes(TimeUnit.NANOSECONDS.toHours(difference))), (TimeUnit.NANOSECONDS.toSeconds(difference) - TimeUnit.MINUTES.toSeconds(TimeUnit.NANOSECONDS.toMinutes(difference))))
      //  println(readandBroadcastPincodes_str)
      pincodeBroadcast
    }

    def fetchMobileEmailAddressFromUniwareQuery(uniwareDBUserName: String, uniwareDBPassword: String, fromInclusiveDate: String, tillExclusiveDate: String, jdbcURL: String, databaseName: String): String = {
      var connection: Connection = null
      var isDistrictColAbsent: Boolean = false
      try {
        val driver = "com.mysql.jdbc.Driver"
        Class.forName(driver)
        connection = DriverManager.getConnection(jdbcURL, uniwareDBUserName, uniwareDBPassword)
        val statement = connection.createStatement()
        val query = "SELECT TABLE_SCHEMA FROM information_schema.COLUMNS WHERE TABLE_SCHEMA ='" + databaseName + "'  AND TABLE_NAME='address_detail' AND COLUMN_NAME = 'district';"
        val resultSet = statement.executeQuery(query)
        if (resultSet.next() == false) {
          isDistrictColAbsent = true
          //      println(jdbcURL+ " isDistrictColAbsent")
        }
      } catch {
        case e: Exception => throw e
      }
      var fetchQuery = emptyStringBroadcast.value
      if (isDistrictColAbsent) {
        fetchQuery =
          """(SELECT ad.name,ad.phone AS mobile, so.notification_mobile AS notification_mobile,ad.email AS email,
            |       so.notification_email  AS notification_email,
            |       ad.address_line1       AS address_line1,
            |       ad.address_line2       AS address_line2,
            |       ad.city                AS city,
            |       ""                     AS district,
            |       ad.state_code          AS state_code,
            |       ad.country_code        AS country_code,
            |       ad.pincode             AS pincode,
            |       sp.created             AS uniware_sp_created,
            |       sp.updated             AS uniware_sp_updated,
            |       tenant.code            AS tenant_code,
            |       party.code             AS facility_code,
            |       sp.code                AS shipping_package_code,
            |       ""                     AS channel_source_code,
            |       sp.status_code         AS shipping_package_uc_status,
            |       so.code                AS sale_order_code,
            |       so.status_code              AS sale_order_uc_status,
            |       "UNKNOWN"                   AS sale_order_turbo_status
            |FROM   shipping_package sp
            |        STRAIGHT_JOIN address_detail ad
            |            ON ad.id = sp.shipping_address_id
            |        STRAIGHT_JOIN sale_order so
            |            ON sp.sale_order_id = so.id
            |        STRAIGHT_JOIN tenant
            |            ON tenant.id = so.tenant_id
            |        STRAIGHT_JOIN facility
            |            ON facility.id = sp.facility_id
            |        STRAIGHT_JOIN party
            |            ON facility.id = party.id
            | WHERE sp.status_code IN ('CANCELLED','RETURNED','RETURN_ACKNOWLEDGED','RETURN_EXPECTED','SHIPPED','DISPATCHED','MANIFESTED','DELIVERED')
            |       AND
            |        tenant.code NOT IN ('lenskart91', 'lenskartcom77', 'lenskartcom97','myntracom70','myntracom73')
            |       AND
            |        sp.created >=""".stripMargin + fromInclusiveDate +
            """
              |       AND
              |        sp.created <""".stripMargin + tillExclusiveDate +
            """
              |       AND
              |        ad.country_code = "IN"
              |       AND
              |        ad.phone NOT LIKE '*%'
              |       AND
              |        ad.phone NOT IN ( '9999999999', '0000000000', '8888888888','1111111111','9898989898', '0123456789', '1234567890',
              |                              '0987654321','09999999999' )) as foo""".stripMargin
      } else {
        fetchQuery =
          """(SELECT ad.name,
            |       ad.phone               AS mobile,
            |       so.notification_mobile AS notification_mobile,
            |       ad.email               AS email,
            |       so.notification_email  AS notification_email,
            |       ad.address_line1       AS address_line1,
            |       ad.address_line2       AS address_line2,
            |       ad.city                AS city,
            |       ad.district            AS district,
            |       ad.state_code          AS state_code,
            |       ad.country_code        AS country_code,
            |       ad.pincode             AS pincode,
            |       sp.created             AS uniware_sp_created,
            |       sp.updated             AS uniware_sp_updated,
            |       tenant.code            AS tenant_code,
            |       party.code             AS facility_code,
            |       sp.code                AS shipping_package_code,
            |       ad.channel_source_code AS channel_source_code,
            |       sp.status_code         AS shipping_package_uc_status,
            |       so.code                AS sale_order_code,
            |       so.status_code              AS sale_order_uc_status,
            |       "UNKNOWN"                  AS sale_order_turbo_status
            |FROM   shipping_package sp
            |        STRAIGHT_JOIN address_detail ad
            |            ON ad.id = sp.shipping_address_id
            |        STRAIGHT_JOIN sale_order so
            |            ON sp.sale_order_id = so.id
            |        STRAIGHT_JOIN tenant
            |            ON tenant.id = so.tenant_id
            |        STRAIGHT_JOIN facility
            |            ON facility.id = sp.facility_id
            |        STRAIGHT_JOIN party
            |            ON facility.id = party.id
            | WHERE sp.status_code IN ('CANCELLED','RETURNED','RETURN_ACKNOWLEDGED','RETURN_EXPECTED','SHIPPED','DISPATCHED','MANIFESTED','DELIVERED')
            |       AND
            |        tenant.code NOT IN ('lenskart91', 'lenskartcom77', 'lenskartcom97','myntracom70','myntracom73')
            |       AND
            |        sp.created >=""".stripMargin + fromInclusiveDate +
            """
              |       AND
              |        sp.created <""".stripMargin + tillExclusiveDate +
            """
              |       AND
              |        ad.country_code = "IN"
              |       AND
              |        ad.phone NOT LIKE '*%'
              |       AND
              |        ad.phone NOT IN ( '9999999999', '0000000000', '8888888888','1111111111','9898989898', '0123456789', '1234567890',
              |                              '0987654321','09999999999' )) as foo""".stripMargin
      }
      fetchQuery
    }

    /**
     * NOTE: min, max only controls single partition size = (upper bound - lower bound)/numPartitions
     * min, max does not controls data fetch boundary
     */
    def anticipateDataSize(jdbcURL: String): (Long, Long) = {
      val uniwareDBUserName = "root"
      val uniwareDBPassword = "UnI@#3D"

      val minMaxAddIdView = "(select min(id) as min ,max(id) as max from address_detail) def"
      val minMaxRow = sparkSession.sqlContext.read.format("jdbc").option("driver", "com.mysql.cj.jdbc.Driver").option("url", jdbcURL).option("user", uniwareDBUserName).option("password", uniwareDBPassword).option("dbtable", minMaxAddIdView).load().collectAsList().get(0)
      val min = minMaxRow.getAs[Long]("min")
      val max = minMaxRow.getAs[Long]("max")
      (min, max)
    }

    /**
     * NOTE: min, max only controls single partition size = (upper bound - lower bound)/numPartitions
     * min, max does not controls data fetch boundary
     */
    def readUniwareJDBC(userName: String, password: String, fromInclusiveDate: String, tillExclusiveDate: String, servername: String, databaseName: String): Dataset[UniwareShippingPackage] = {
      val fetchSizeForJDBCRead = "50000"
      val numPartitionsForJDBCRead: String = "20"
      val jdbcURL = jdbcURLForUniwareServerName(servername, databaseName)
      val min_max_pair = anticipateDataSize(jdbcURL)
      val min = min_max_pair._1
      val max = min_max_pair._2
      import sparkSession.implicits._
      var df_withNumPartitions: Dataset[UniwareShippingPackage] = sparkSession.emptyDataset[UniwareShippingPackage]
      if (min != max) {
        val fetchQuery = fetchMobileEmailAddressFromUniwareQuery(userName, password, fromInclusiveDate, tillExclusiveDate, jdbcURL, databaseName)
        df_withNumPartitions = sparkSession.sqlContext.read.format("jdbc").option("driver", "com.mysql.cj.jdbc.Driver").option("url", jdbcURL).option("user", userName).option("password", password).option("dbtable", fetchQuery).option("fetchSize", fetchSizeForJDBCRead).load().na.fill("").as[UniwareShippingPackage]
      }
      df_withNumPartitions
    }

    def readJDBCTerminatedTenants(fromInclusiveDate: String, tillExclusiveDate: String, terminatedDBServer: String): scala.collection.mutable.Set[String] = {
      import sparkSession.implicits._
      val excludedDBSet = Set("information_schema", "performance_schema", "mysql", "sys", "Discernliving")
      val uniwareDBUserName = "developer"
      val uniwareDBPassword = "DevelopeR@4#"
      val jdbcURL = jdbcURLForUniwareServerName(terminatedDBServer, "")
      val query =
        "select terminated_server from terminated_info.terminated_info where type='production' and terminated_server not in (SELECT S.SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA S LEFT OUTER JOIN INFORMATION_SCHEMA.TABLES T ON S.SCHEMA_NAME = T.TABLE_SCHEMA WHERE T.TABLE_SCHEMA IS NULL) and (terminated_on IS NULL OR (terminated_on>=" + fromInclusiveDate + " and terminated_on<" + tillExclusiveDate + "))"
      var connection: Connection = null
      var terminatedTenantDBSet = scala.collection.mutable.Set[String]()
      try {
        val driver = "com.mysql.jdbc.Driver"
        Class.forName(driver)
        connection = DriverManager.getConnection(jdbcURL, uniwareDBUserName, uniwareDBPassword)
        val statement = connection.createStatement()
        val resultSet = statement.executeQuery(query)

        while (resultSet.next()) {
          var next: String = resultSet.getString("terminated_server")
          if (!excludedDBSet.contains(next)) {
            terminatedTenantDBSet.add(next)
          }
        }
      } finally {
        connection.close()
      }
      print("terminatedTenantDBSet:" + terminatedTenantDBSet)
      terminatedTenantDBSet
    }

    //def getAddFingerPrint(mobile: String, uniwareAddress: UniwareShippingPackage): String = {
    //  val mobile_mapped_add_fingerprint = StringBuilder.newBuilder
    //  mobile_mapped_add_fingerprint.append(mobile)
    //  mobile_mapped_add_fingerprint.append(uniwareAddress.address_line1)
    //  mobile_mapped_add_fingerprint.append(uniwareAddress.address_line2)
    //  mobile_mapped_add_fingerprint.append(uniwareAddress.city)
    //  mobile_mapped_add_fingerprint.append(uniwareAddress.district)
    //  mobile_mapped_add_fingerprint.append(uniwareAddress.state_code)
    //  mobile_mapped_add_fingerprint.append(uniwareAddress.pincode)
    //  removeSpaceAndLowerCase(mobile_mapped_add_fingerprint.toString())
    //}

    def isEmailSyntaxValid_RFC822(email: String): Boolean = {
      try {
        new InternetAddress(email).validate()
        return true;
      } catch {
        case _: Throwable => return false
      }
    }

    def removeControlChar(str: String) = CharMatcher.JAVA_ISO_CONTROL.removeFrom(str);

    def cleanupNameCityDistrictAddress(field: String): String = {
      if (field != null) {
        if (List(slashNBroadcastString.value, nanBroadcastString.value).contains(field))
          return emptyStringBroadcast.value
        else {
          var cleanString = field.trim()
          if (cleanString.endsWith(commaBroadcastString.value)) {
            cleanString = cleanString.substring(0, cleanString.length - 1)
          }
          return removeControlChar(cleanString)
        }
      } else {
        return emptyStringBroadcast.value
      }
    }

    def isCountryIndia(country_code: String): Boolean = {
      try {
        return country_code.equalsIgnoreCase(countryBroadcastString.value)
      } catch {
        case _: Throwable =>
      }
      return false
    }

    def isIndiaPincode(pincode: String, pincodeBroadcast: Broadcast[Set[String]], compiledValidPincodePattern: Pattern): Boolean = {
      return pincodeBroadcast.value.contains(pincode) || isValidPinCodeAsPerRegex(pincode, compiledValidPincodePattern)
    }

    def isValidIndiaAddressCode(row: UniwareShippingPackage, pincodeBroadcast: Broadcast[Set[String]]): Option[UniwareShippingPackage] = {
      val formattedCountryCode = removeControlChar(removeSpace(row.country_code)).toUpperCase()
      //      val isAddCntryCodeIN = !isEmpty(formattedCountryCode) && isCountryIndia(formattedCountryCode)
      val formattedPincodeCode = removeControlChar(removeSpace(row.pincode))
      val isValidInPinCode = !isEmpty(formattedPincodeCode) && isIndiaPincode(formattedPincodeCode, pincodeBroadcast, compiledValidPincodePattern)
      if (isValidInPinCode)
        Some(row.copy(name = removeControlChar(row.name), mobile = removeControlChar(removeSpace(row.mobile)), notification_mobile = removeControlChar(removeSpace(row.notification_mobile)), email = removeControlChar(removeSpace(row.email)), notification_email = removeControlChar(removeSpace(row.notification_email)), address_line1 = cleanupNameCityDistrictAddress(row.address_line1), address_line2 = cleanupNameCityDistrictAddress(row.address_line2), city = cleanupNameCityDistrictAddress(row.city), district = cleanupNameCityDistrictAddress(row.district), state_code = cleanupNameCityDistrictAddress(row.state_code), country_code = formattedCountryCode, pincode = formattedPincodeCode))
      else
        None
    }

    def uniqueValidEmailsSet(email: String, notification_email: String): Set[String] = {
      val validEmail_set = scala.collection.mutable.Set[String]()
      if (!isEmpty(email) && isEmailSyntaxValid_RFC822(email)) {
        validEmail_set.add(email)
      }
      if (!isEmpty(notification_email) && isEmailSyntaxValid_RFC822(notification_email)) {
        validEmail_set.add(notification_email)
      }
      return validEmail_set.toSet
    }

    def uniqueValidMobilesSet(mobile_raw: String, notification_mobile_raw: String, doRecovery: Boolean): Set[String] = {
      val toProcessSet: scala.collection.mutable.Set[String] = scala.collection.mutable.Set[String]()
      Set(mobile_raw, notification_mobile_raw).foreach((p: String) => {
        var validFormattedPhone: List[String] = List()
        if (doRecovery) {
          validFormattedPhone = recover_FIXED_LINE_OR_MOBILE_Pattern(p)
        } else {
          validFormattedPhone = no_recover(p)
        }
        if (!validFormattedPhone.isEmpty) validFormattedPhone.foreach(toProcessSet.add(_))
      })
      return toProcessSet.toSet
    }

    def explodeRow(row: UniwareShippingPackage, pincodeBroadcast: Broadcast[Set[String]], doRecovery: Boolean): Array[ExplodedUniwareShippingPackage] = {
      val rowsUnifill = new ArrayBuffer[ExplodedUniwareShippingPackage]()
      val validAddRowOpt: Option[UniwareShippingPackage] = isValidIndiaAddressCode(row, pincodeBroadcast)
      if (validAddRowOpt.isDefined) {
        val validAddRow: UniwareShippingPackage = validAddRowOpt.get
        val validMobilesInRow = uniqueValidMobilesSet(validAddRow.mobile, validAddRow.notification_mobile, doRecovery)
        val validEmailsInRow = uniqueValidEmailsSet(validAddRow.email, validAddRow.notification_email)
        if (validMobilesInRow.size == 0 && validEmailsInRow.size != 0) {
          validEmailsInRow.foreach(email_unifill => rowsUnifill.append(ExplodedUniwareShippingPackage(true, current_date_timeBroadcast.value, current_date_timeBroadcast.value, emptyStringBroadcast.value, email_unifill, validAddRow)))
        } else if (validMobilesInRow.size != 0 && validEmailsInRow.size == 0) {
          validMobilesInRow.foreach(mobile_unifill => {
            rowsUnifill.append(ExplodedUniwareShippingPackage(true, current_date_timeBroadcast.value, current_date_timeBroadcast.value, mobile_unifill, emptyStringBroadcast.value, validAddRow))
          })
        } else {
          validMobilesInRow.foreach(mobile_unifill => {
            validEmailsInRow.foreach(email_unifill => rowsUnifill.append(ExplodedUniwareShippingPackage(true, current_date_timeBroadcast.value, current_date_timeBroadcast.value, mobile_unifill, email_unifill, validAddRow)))
          })
        }
      }
      rowsUnifill.toArray
    }

    def writeOverJDBC(sparkSession: SparkSession, validAddWithMappedValidMobileOrEmail: Dataset[ExplodedUniwareShippingPackage]) = {

      val url = getClass.getResource("application.properties")
      val properties: Properties = new Properties()

      if (url != null) {
        val source = Source.fromURL(url)
        properties.load(source.bufferedReader())
      }
      else {
        println("Properties file cannot be loaded")
        throw new FileNotFoundException("Properties file cannot be loaded");
      }

      val destJDBCURL: String = properties.getProperty("unifill.datasource.url") + "?useSSL=false&useServerPrepStmts=false&rewriteBatchedStatements=true&enabledTLSProtocols=TLSv1.3"
      val batchSizeForJDBCWrite = properties.getProperty("unifill.datasource.batch.size.jdbc.write")
      val destTable: String = properties.getProperty("unifill.datasource.table")
      val destDBUserName: String = properties.getProperty("unifill.datasource.username")
      val destDBPassword: String = properties.getProperty("unifill.datasource.password")

      val flattenedDF: DataFrame = validAddWithMappedValidMobileOrEmail.select("enabled", "turbo_created", "turbo_updated", "turbo_mobile", "turbo_email", "uniwareShippingPackage.*").drop(Array("mobile", "email", "notification_mobile", "notification_email"): _*)
      flattenedDF.write.format("jdbc").mode(SaveMode.Append).option("driver", "com.mysql.cj.jdbc.Driver").option("url", destJDBCURL).option("dbtable", destTable).option("batchsize", batchSizeForJDBCWrite).option("rewriteBatchedInserts", true).option("isolationLevel", "NONE").option("user", destDBUserName).option("password", destDBPassword).save()
    }

    def transform(row: UniwareShippingPackage, pincodeBroadcast: Broadcast[Set[String]], doRecovery: Boolean): Array[ExplodedUniwareShippingPackage] = {
      val rowsUnifill = explodeRow(row, pincodeBroadcast, doRecovery)
      if (rowsUnifill.isEmpty)
        Array[ExplodedUniwareShippingPackage]()
      else {
        rowsUnifill.toArray
      }
    }

    def transformWrite(serverDF: Dataset[UniwareShippingPackage], spark: SparkSession, pincodeBroadcast: Broadcast[Set[String]], servername: String) = {
      import spark.sqlContext.implicits._
      //  println("Transform write started:" + servername)
      val start = System.nanoTime()
      //    println(servername, serverDF.count())
      val validAddWithMappedValidMobileOrEmail: Dataset[ExplodedUniwareShippingPackage] = serverDF.repartition(500).flatMap(transform(_, pincodeBroadcast, true))
      writeOverJDBC(spark, validAddWithMappedValidMobileOrEmail)
      val end = System.nanoTime()
      val difference = end - start
      val per_server_read_transform_write_log = "Server %s Time-Taken-Read-Transform-Write-hrs %s Time-Taken-Read-Transform-Write-min %s Time-Taken-Read-Transform-Write-sec %s"
      val per_server_read_transform_write_log_str = per_server_read_transform_write_log.format(servername, TimeUnit.NANOSECONDS.toHours(difference), (TimeUnit.NANOSECONDS.toMinutes(difference) - TimeUnit.HOURS.toMinutes(TimeUnit.NANOSECONDS.toHours(difference))), (TimeUnit.NANOSECONDS.toSeconds(difference) - TimeUnit.MINUTES.toSeconds(TimeUnit.NANOSECONDS.toMinutes(difference))))
        println(per_server_read_transform_write_log_str)
        println("Read Transform write completed:" + servername)
    }

    def decryptionProcessingInternal(encryptedText: String, encryptionKey: String): String = {
      val ENCRYPTION_SCHEME = "DESede"
      val UNICODE_FORMAT = "UTF8"
      val AES_ENCRYPTION_SCHEME = "AES/ECB/PKCS5Padding"

      import javax.crypto.Cipher
      import javax.crypto.SecretKey
      import javax.crypto.SecretKeyFactory
      import javax.crypto.spec.DESedeKeySpec
      import javax.crypto.spec.SecretKeySpec


      @throws[InvalidKeyException]
      def getSecretKeyAES(encryptionKey: String): SecretKey = try {
        var keyAsBytes = encryptionKey.getBytes(UNICODE_FORMAT)
        keyAsBytes = util.Arrays.copyOf(keyAsBytes, 256 / 8) // Key length is 16

        new SecretKeySpec(keyAsBytes, "AES")
      } catch {
        case e: Exception =>
          throw new InvalidKeyException("Internal error in generating secret key", e)
      }

      def cryptoUtilsDecryptAES(ciphertext: String, secret: String): String = {
        if (ciphertext == null || "".equals(ciphertext)) return ciphertext
        try {
          val cipher = Cipher.getInstance(AES_ENCRYPTION_SCHEME)
          cipher.init(Cipher.DECRYPT_MODE, getSecretKeyAES(secret))
          new String(cipher.doFinal(Base64.decodeBase64(ciphertext)), "UTF-8")
        } catch {
          case e: Exception =>
            println("[Crypto] Unable to decrypt ciphertext.", e)
            ciphertext
        }
      }


      @deprecated def cryptoUtilsDecrypt(ciphertext: String, encryptionKey: String): String = {
        @deprecated
        @throws[InvalidKeyException]
        def getSecretKey(encryptionKey: String) = try {
          val keyAsBytes = encryptionKey.getBytes(UNICODE_FORMAT)
          val keySpec = new DESedeKeySpec(keyAsBytes)
          val keyFactory = SecretKeyFactory.getInstance(ENCRYPTION_SCHEME)
          keyFactory.generateSecret(keySpec)
        } catch {
          case e: Exception =>
            throw new InvalidKeyException("Internal error in generating secret key", e)
        }
        def getKeyAliasFromEncryptedText(encryptedText: String) = {
          encryptedText.split(":", 3)(2)
        }

        if (ciphertext == null || "".equals(ciphertext)) return ciphertext
        try {
          val cipher = Cipher.getInstance(ENCRYPTION_SCHEME)
          cipher.init(Cipher.DECRYPT_MODE, getSecretKey(encryptionKey))
          val base64decoder = new Base64
          val clearText = base64decoder.decode(ciphertext)
          val cipherText = cipher.doFinal(clearText)
          new String(cipherText, UNICODE_FORMAT)
        } catch {
          case e: Exception =>
            // Unable to decrypt
            println("[Crypto] Unable to decrypt ciphertext.", e)
            ciphertext
        }
      }

      def commonEncryptionServiceDecrypt(cipherText: String,encryptionKey: String): String = {
        val ENCRYPTION_TEXT_PREFIX = "cipher:"
        if (cipherText == null || "".equals(cipherText) || !cipherText.startsWith(ENCRYPTION_TEXT_PREFIX)) return cipherText
        val cipherDetails = cipherText.split(":", 3)
        val keyAlias = cipherDetails(1)
        if (keyAlias.equalsIgnoreCase("1")) {
          cryptoUtilsDecrypt(cipherDetails(2), encryptionKey)
        }
        else if (keyAlias.equalsIgnoreCase("2")) { // Code can be removed after some time, as its method for encryption has been removed
          cipherText
        }
        else {
          val simpleDateFormat = new SimpleDateFormat(DATE_MONTH_YEAR_FORMAT)
//          if (encryptionKey == null) {
//            val indexOfDate = keyAlias.lastIndexOf('_') - 5 // will extract 27_03_2023
//            val isStaticKeyAliasForSource = keyAlias.endsWith("uniware")
//            val currDayKeyAlias = keyAlias.substring(0, indexOfDate) + simpleDateFormat.format(DateUtils.addDaysToDate(new Date, 0))
//            val previousDayKeyAlias = keyAlias.substring(0, indexOfDate) + simpleDateFormat.format(DateUtils.addDaysToDate(new Date, -1))
//            val isSameKeyAlias = keyAlias == currDayKeyAlias || keyAlias == previousDayKeyAlias
//            if (isSameKeyAlias || isStaticKeyAliasForSource) key = clearCacheAndGetEncryptionKey(keyAlias, false)
//          }
          if (encryptionKey == null) cipherDetails(2)
          else cryptoUtilsDecryptAES(cipherDetails(2), encryptionKey)
        }
      }
      commonEncryptionServiceDecrypt(encryptedText, encryptionKey)
    }

//    def decryptionProcessingInternal(ciphertext: String, secret: String) = {
//      val ENCRYPTION_SCHEME = "DESede"
//      val UNICODE_FORMAT = "UTF8"
//      val AES_ENCRYPTION_SCHEME = "AES/ECB/PKCS5Padding"
//
//      @throws[InvalidKeyException]
//      def getSecretKeyAES(encryptionKey: String) = try {
//        var keyAsBytes = encryptionKey.getBytes(UNICODE_FORMAT)
//        keyAsBytes = util.Arrays.copyOf(keyAsBytes, 256 / 8) // Key length is 16
//
//        new SecretKeySpec(keyAsBytes, "AES")
//      } catch {
//        case e: Exception =>
//          throw new InvalidKeyException("Internal error in generating secret key", e)
//      }
//
//      try {
//        val cipher = Cipher.getInstance(AES_ENCRYPTION_SCHEME)
//        cipher.init(Cipher.DECRYPT_MODE, getSecretKeyAES(secret))
//        new String(cipher.doFinal(Base64.decodeBase64(ciphertext)), "UTF-8")
//      } catch {
//        case e: Exception =>
//          println("[decryptionProcessingInternal] Unable to decrypt ciphertext.", e)
//          ciphertext
//      }
//    }

    def decryptionProcessing(encryptedDF: Dataset[UniwareShippingPackage]) = {

      // 1. distinct values of mobile where "isPIIEncryptedText method" ==> distinct keyAlias using "getKeyAliasFromEncryptedText method
      import org.apache.spark.sql.functions.{col}
      import org.apache.spark.sql.functions._
      val encryptedDFWithKeyAlias = encryptedDF.withColumn("keyAlias", split(col("mobile"), ":").getItem(1))
      val distinct_tenantCode_keyAliasDF: DataFrame = encryptedDF.select("mobile", "tenant_code").repartition(1).
        withColumn("keyAlias", split(col("mobile"), ":").getItem(1)).dropDuplicates("keyAlias", "tenant_code").select("tenant_code", "keyAlias")
      distinct_tenantCode_keyAliasDF.show(truncate = false)
      println("INSIDE [decryptionProcessing] distinct_tenantCode_keyAliasDF count:" + distinct_tenantCode_keyAliasDF.count())

      // 2. Fetch vault key and create new dataframe
      var vaultKeySeq: Seq[VaultKey] = Seq[VaultKey]()
      distinct_tenantCode_keyAliasDF.collectAsList().forEach(row => {
        val key = PIIUtils.getEncryptionKey(row.getAs("keyAlias"), row.getAs("tenant_code"))
        println("key for " + row.getAs("keyAlias") + ":" + row.getAs("tenant_code") + " is:"  + key)
        vaultKeySeq = vaultKeySeq :+ VaultKey(row.getAs("tenant_code"), row.getAs("keyAlias"), key)
      })
      import sparkSession.sqlContext.implicits._
      val distinct_tenantCode_keyAlias_VaultKeyDF = vaultKeySeq.toDF("tenant_code", "keyAlias", "vaultKey")
      println("INSIDE [decryptionProcessing] distinct_tenantCode_keyAlias_VaultKeyDF count:" + distinct_tenantCode_keyAlias_VaultKeyDF.count())

      //3. join
      val joinedDF = encryptedDFWithKeyAlias.join(distinct_tenantCode_keyAlias_VaultKeyDF,Seq("tenant_code", "keyAlias"))
      println("INSIDE [decryptionProcessing] joinedDF count:" + joinedDF.count())
      val decryptUDF = udf(decryptionProcessingInternal(_, _))
      joinedDF.select(col("vaultKey"), col("tenant_code"), col("mobile")).show(truncate = false)
      val postDecryptDF = joinedDF.
        withColumn("name", decryptUDF.apply(col("name"), col("vaultKey"))).
        withColumn("mobile", decryptUDF.apply(col("mobile"), col("vaultKey"))).
        withColumn("notification_mobile", decryptUDF.apply(col("notification_mobile"), col("vaultKey"))).
        withColumn("email", decryptUDF.apply(col("email"), col("vaultKey"))).
        withColumn("notification_email", decryptUDF.apply(col("notification_email"), col("vaultKey"))).
        withColumn("address_line1", decryptUDF.apply(col("address_line1"), col("vaultKey"))).
        withColumn("address_line2", decryptUDF.apply(col("address_line2"), col("vaultKey")))
      postDecryptDF.show(truncate = false)
      postDecryptDF.select("name", "mobile", "notification_mobile", "email", "notification_email", "address_line1",
        "address_line2", "city", "district", "state_code", "country_code", "pincode", "uniware_sp_created",
        "uniware_sp_updated", "tenant_code", "facility_code", "shipping_package_code", "channel_source_code",
        "shipping_package_uc_status", "sale_order_code", "sale_order_uc_status", "sale_order_turbo_status").as[UniwareShippingPackage]
    }

    /**
     * https://stackoverflow.com/questions/48276241/spark-difference-between-numpartitions-in-read-jdbc-numpartitions-and-repa
     */
    def readTransformWrite(userName: String, password: String, fromInclusiveDate: String, tillExclusiveDate: String, spark: SparkSession, pincodeBroadcast: Broadcast[Set[String]], servername: String, databaseName: String) = {
      import spark.sqlContext.implicits._
      //  println("Read Transform write started, servername:" + servername + " databaseName: " + databaseName )
      val start = System.nanoTime()
      val serverDF: Dataset[UniwareShippingPackage] = readUniwareJDBC(userName, password, fromInclusiveDate, tillExclusiveDate, servername, databaseName).as[UniwareShippingPackage]
      println("Starting PII handling, servername: " + servername + " serverDF.count(): " + serverDF.count())
      val condition =  col("mobile").isNotNull and col("mobile").startsWith("cipher:")
      val encryptedDF = serverDF.filter(condition)
      println("servername: " + servername + " encryptedDF.count() " + encryptedDF.count())
      val plainDF = serverDF.filter(!condition)
      println("servername: " + servername + " plainDF.count() " + plainDF.count())

      var decryptedDF = sparkSession.emptyDataset[UniwareShippingPackage]
      if(encryptedDF.isEmpty == false) {
        decryptedDF = decryptionProcessing(encryptedDF)
        println("servername: " + servername + " decryptedDF.count() " + decryptedDF.count())
      }

      val postUnionDF = plainDF.union(decryptedDF)
      println("servername: " + servername + " postUnionDF.count() " + postUnionDF.count())
      println("servername: " + servername +  " completed PII handling, going for transformWrite")
      transformWrite(postUnionDF, spark, pincodeBroadcast, servername)
    }

    def readProdServers(): Set[String] = {
      val prodServerSet = scala.collection.mutable.Set[String]()
      val mongoClient: MongoClient = MongoClients.create("mongodb://common1.mongo.unicommerce.infra:27017/uniwareConfig.serverDetails")
      val database: MongoDatabase = mongoClient.getDatabase("uniwareConfig");
      val collection: MongoCollection[Document] = database.getCollection("serverDetails");
      try {
        val docs: DistinctIterable[String] = collection.distinct("db", Filters.and(Filters.eq("production", "true"), Filters.eq("active", "true")), classOf[String])
        val results: MongoCursor[String] = docs.iterator();
        while (results.hasNext()) {
          prodServerSet += results.next();
        }
      } catch {
        case e: MongoException => {
          System.err.println("An error occurred: " + e)
          System.exit(1)
        }
      }
      prodServerSet.toSet
    }

    /**
     * JDBC READ
     * 1. no of tasks(for corresponding spark job->stage) === equal to no of cores available to spark cluster/sc.defaultParallelism
     * 2. note: IF OOM comes, increase the numbers of partitions beyond the number of available executors
     * 3. just for double checking, configured partitions === actual partitions(made by spark) === number of cores(not no of executors)
     *
     * JDBC WRITE
     * write numPartitions - not configured, will be same as of numPartitionsForJDBCRead if no repartition is called
     *
     * Peformance best practise
     * AVOID RE EXECUTION OF SPARK JOB/STAGES
     *
     */
    def readTransfromWriteInParallel(fromInclusiveDate: String, tillExclusiveDate: String, excludeServers: Set[String], terminatedDBServer: String) = {
      val prodServerSet = readProdServers().diff(excludeServers)
      val pincodeBroadcast: Broadcast[Set[String]] = readandBroadcastPincodes()
      var listThreads: ListBuffer[Thread] = ListBuffer[Thread]()

      for (servername: String <- prodServerSet) {
        val thread = new Thread {
          val userName = "developer"
          val password = "DevelopeR@4#"

          override def run: Unit = readTransformWrite(userName, password, fromInclusiveDate, tillExclusiveDate, sparkSession, pincodeBroadcast, servername, "uniware")
        }
        thread.start()
        listThreads.append(thread)
      }

      val terminatedDBSet = readJDBCTerminatedTenants(fromInclusiveDate, tillExclusiveDate, terminatedDBServer)
      for (dbName: String <- terminatedDBSet) {
        val thread = new Thread {
          val uniwareDBUserName = "developer"
          val uniwareDBPassword = "DevelopeR@4#"

          override def run: Unit = readTransformWrite(uniwareDBUserName, uniwareDBPassword, fromInclusiveDate, tillExclusiveDate, sparkSession, pincodeBroadcast, terminatedDBServer, dbName)
        }
        thread.start()
        listThreads.append(thread)
      }

      for (thread <- listThreads) {
        thread.join()
      }
    }
    readTransfromWriteInParallel(fromInclusiveDate, tillExclusiveDate, excludeServers, terminatedDBServer)
  }
}