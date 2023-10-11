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
import java.io.{IOException, Serializable}
import java.util
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
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
import org.springframework.http.HttpStatus

import java.io.IOException
import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.security.InvalidKeyException
import javax.crypto.Cipher
//import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64

import java.util.concurrent.TimeUnit
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


  object PIIVaultConfig extends VaultConfig {
    val clientToLoginToken: collection.mutable.Map[String, String] = collection.mutable.Map[String, String]()

    override def address(address: String): VaultConfig = {
      println("Inside [PIIVaultConfig][address] address" + address)
      println("Inside [PIIVaultConfig][address] setting default address" + "http://internal-HAVaultProdLB-198417283.ap-southeast-1.elb.amazonaws.com")
      super.address("http://internal-HAVaultProdLB-198417283.ap-southeast-1.elb.amazonaws.com").build()
    }
    val clientToPassword: collection.mutable.Map[String, String] = {
      val map = collection.mutable.Map[String, String]()
      println("Inside [PIIVaultConfig] populating clientToPassword")
      val lines = Source.fromFile("/spark/deployment/target/active_tenant.txt").getLines
      lines.foreach(line => {
        val lineArr = line.split(":",2)(1).split(":")
        map.put(lineArr(0), lineArr(1))
      })
      map
    }

    override def getToken() = {
      val clientCode = EncryptionContext.current.getClientCode()
      println("Inside [PIIVaultConfig][getToken], clientCode" + clientCode)
      if (clientToLoginToken.contains(clientCode)) {
        println("Inside [PIIVaultConfig][getToken], clientToLoginToken.getOrElse(clientCode, null)" + clientToLoginToken.getOrElse(clientCode, null))
        clientToLoginToken.getOrElse(clientCode, null)
      } else {
        val password = clientToPassword.getOrElse(clientCode, "")
        println("Inside [PIIVaultConfig][getToken], clientToLoginToken does not contain token, client password:" + password)
        try {
          println("Inside [PIIVaultConfig][fetchVaultLoginTokenInternal], tenantCode:" + clientCode + " tenantPassword: " + password)
          val authResponse = PIIUtils.vault.auth.loginByUserPass(clientCode, password)
          println("inside [PIIUtils][fetchVaultLoginTokenInternal], token", authResponse.getAuthClientToken)
          clientToLoginToken.put(clientCode, authResponse.getAuthClientToken)
          println("inside [PIIUtils][fetchVaultLoginTokenInternal], AUTH done, token saved cached memory")
          authResponse.getAuthClientToken
        } catch {
          case e: VaultException =>
            println("inside [PIIUtils][fetchVaultLoginTokenInternal], Error while authenticating client {}", clientCode, e)
            throw new UniVaultException(e)
        }
      }
    }
  }


