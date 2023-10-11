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

object PIIUtils extends Serializable {
  val vault = new Vault(PIIVaultConfig.address("http://internal-HAVaultProdLB-198417283.ap-southeast-1.elb.amazonaws.com"), 1)
  val EXPORT_KEY_PATH_PREFIX = "transit/export/encryption-key/"
  val DEFAULT_VERSION = "1"
  val VAULT_ENGINE_VERSION = 1
  val objectMapper: ObjectMapper = new ObjectMapper()
  val ENCRYPTION_TEXT_PREFIX = "cipher:"

  def fetchVaultLoginTokenInternal(tenantCode: String, tenantPassword: String): String = {
    try {
      println("Inside [PIIVaultConfig][fetchVaultLoginTokenInternal], tenantCode:" + tenantCode + " tenantPassword: " + tenantPassword)
      val authResponse = vault.auth.loginByUserPass(tenantCode, tenantPassword)
      println("inside [PIIUtils][fetchVaultLoginTokenInternal], token", authResponse.getAuthClientToken)
      PIIVaultConfig.clientToLoginToken.put(tenantCode, authResponse.getAuthClientToken)
      println("inside [PIIUtils][fetchVaultLoginTokenInternal], AUTH done, token saved cached memory")
      authResponse.getAuthClientToken
    } catch {
      case e: VaultException =>
        println("inside [PIIUtils][fetchVaultLoginTokenInternal], Error while authenticating client {}", tenantCode, e)
        throw new UniVaultException(e)
    }
  }

  def get(path: String, loginToken: String, tenantCode: String, tenantPassword: String): LogicalResponse = {
    println("inside [PIIUtils][get], Hitting GET request on path {}", path)
    EncryptionContext.current.setClientCode(tenantCode)
    if (loginToken == null || "".equals(loginToken)) {
      println("inside get, going to fetch auth token for tenantCode" + tenantCode)
      fetchVaultLoginTokenInternal(tenantCode, tenantPassword)
    }

    try {
      println("inside get, going to fetch key, tenantCode" + tenantCode)
      vault.logical.read(path)
    }
    catch {
      case e: VaultException =>
        println("[Vault] Error occured.", e)
        // Retrying if authentication expired.
        if (e.getHttpStatusCode == HttpStatus.FORBIDDEN.value()) {
          println("[Vault] Token Expired. Authenticating Again...")
          fetchVaultLoginTokenInternal(tenantCode, tenantPassword)
          try return vault.logical.read(path)
          catch {
            case e1: VaultException =>
              if (e1.getHttpStatusCode == HttpStatus.NOT_FOUND.value()) {
                println("[Vault] Key not found after re-authentication", e1)
                throw new VaultKeyNotFoundException(e1)
              }
              println("[Vault] Error occurred after re-authentication.", e1)
              throw new UniVaultException(e1)
          }
        }
        else if (e.getHttpStatusCode == HttpStatus.NOT_FOUND.value()) {
          println("[Vault] Key not found", e)
          throw new VaultKeyNotFoundException(e)
        }
        throw new UniVaultException("[Vault] Internal Server Error", e)
    }
  }

  def isEncryptedText(cipherText: String): Boolean = {
    if (cipherText == null || "".equals(cipherText) || !cipherText.startsWith(ENCRYPTION_TEXT_PREFIX))
      false
    else
      true
  }

  def isPIIEncryptedText(cipherText: String): Boolean = {
    val keyAlias: String = getKeyAliasFromEncryptedText(cipherText)
    isEncryptedText(cipherText) && !keyAlias.equalsIgnoreCase("1") && !keyAlias.equalsIgnoreCase("2")
  }

  def getKeyAliasFromEncryptedText(cipherText: String): String = {
    cipherText.split(":", 3)(1)
  }

  def getVersionToEncryptionKeyMap(versionToKeyString: String) = try {
    val keyMapType = new TypeReference[util.Map[String, String]]() {}
    objectMapper.readValue(versionToKeyString, keyMapType)
  } catch {
    case e: IOException =>
      throw new UniVaultException(e)
  }

  def getExportEncryptionKeyPathString(version: String, tenantCode: String) = {
    EXPORT_KEY_PATH_PREFIX + tenantCode + "_" + "uniware" + "/" + version
  }

  def getEncryptionKeyForPIIData(keyAlias: String, tenantCode: String, createKeyIfNotFound: Boolean): String = {
    val keyPath = EXPORT_KEY_PATH_PREFIX + keyAlias + "/" + DEFAULT_VERSION
    var versionToKeyString: String = null
    val tenantPassword = PIIVaultConfig.clientToPassword(tenantCode)
    try {
      println("Fetching keys from vault for keyAlias {}", keyAlias)
      versionToKeyString = get(keyPath, null, tenantCode, tenantPassword).getData.get("keys")
    } catch {
      case e: VaultKeyNotFoundException =>
        if (!createKeyIfNotFound) return null
        else { // if key not found at vault (task couldn't be run at 12AM), so key will be created at first encryption
          //            println("Creating keys in vault for keyAlias {}", keyAlias)
          //            vaultService.createKey(keyAlias)
          //            vaultService.allowKeyDeletion(keyAlias)
          //            versionToKeyString = vaultService.get(keyPath).getData.get("keys")
          //            PIIEncryptionContext.setContext
        }
    }
    val versionToKeyMap = getVersionToEncryptionKeyMap(versionToKeyString)
    if (versionToKeyMap.isEmpty) throw new UniVaultException("Invalid Key Alias - " + keyAlias)
    val keyEntrySet = versionToKeyMap.entrySet.iterator.next
    keyEntrySet.getValue
  }

  def getEncryptionKeyForNonPIIData(keyAlias: String, tenantCode: String): String = {
    val tenantPassword = PIIVaultConfig.clientToPassword(tenantCode)
    println("Fetching keys from vault for tenant")
    val versionToKeyString = get(getExportEncryptionKeyPathString(keyAlias, tenantCode), null, tenantCode, tenantPassword).getData.get("keys")
    val versionToKeyMap = getVersionToEncryptionKeyMap(versionToKeyString)
    if (versionToKeyMap.isEmpty) throw new UniVaultException("Invalid Key Version - " + keyAlias)
    // Returning the first key as we are fetching one key at a time.
    val keyEntrySet = versionToKeyMap.entrySet.iterator.next
    keyEntrySet.getValue
  }

  def getEncryptionKey(keyAlias: String, tenantCode: String) = {
    if (keyAlias.equalsIgnoreCase("1")) {
      getEncryptionKeyForNonPIIData(keyAlias, tenantCode)
    }
    else if (keyAlias.equalsIgnoreCase("2")) { // Code can be removed after some time, as its method for encryption has been removed
      null
    } else {
      getEncryptionKeyForPIIData(keyAlias, tenantCode, false)
    }
  }

  //  def main(args: Array[String]): Unit = {
  //
  //    // 1. setup test data into df1
  //    val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
  //    import sqlcontext.implicits._
  //    var df1 = sqlcontext.
  //      createDataFrame(
  //        Seq(("cipher:mosaicwellnesspvtlmt_WOOCOMMERCE_uniware:miYv1eSta8gZeoZwPEyYIQ==","mosaicwellnesspvtlmt_WOOCOMMERCE_uniware", "mosaicwellnesspvtlmt","miYv1eSta8gZeoZwPEyYIQ=="))
  //      ).
  //      toDF("mobile","keyAlias", "tenantCode","encryptedText")
  //
  //    def isEncryptedTextUDF = udf(PIIUtils.isEncryptedText(_))
  //    df1.select("mobile","tenantCode").repartition(1).filter(isEncryptedTextUDF(col("mobile")) === true).
  //      withColumn("keyAlias",split(col("mobile"), ":").getItem(1)).dropDuplicates("keyAlias", "tenantCode").select("tenantCode","keyAlias").show(truncate=false)
  //
  //
  //    //2. Get vault key for test data into df2
  //    var vaultKeySeq: Seq[VaultKey] = Seq[VaultKey]()
  //    df1.collectAsList().forEach(row => {
  //      val key = PIIUtils.getEncryptionKey( row.getAs("keyAlias"), row.getAs("tenantCode"))
  //      vaultKeySeq = vaultKeySeq :+ VaultKey(row.getAs("tenantCode"), row.getAs("keyAlias"), key)
  //    })
  //    val df2 = vaultKeySeq.toDF("tenantCode","keyAlias","vaultKey")
  //    df2.show(truncate = false)
  //
  //    // 3. join df1 and df2
  //    val df3 = df1.join(df2,
  //      df1("tenantCode") === df2("tenantCode") &&
  //        df1("keyAlias") === df2("keyAlias")
  //    )
  //    println("JOINED DF show")
  //    df3.select(col("vaultKey"), col("encryptedText")).show(truncate = false)
  //
  //    // 4.create new df4 with decrypt data column
  //    def decrypt(ciphertext: String, secret: String): String = {
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
  //          println("[Crypto] Unable to decrypt ciphertext.", e)
  //          ciphertext
  //      }
  //    }
  //    val decryptUDF = udf(decrypt(_, _))
  //    val df4 = df3.select("encryptedText","vaultKey").withColumn("plainText", decryptUDF.apply(col("encryptedText"),col("vaultKey")))
  //    df4.select(col("vaultKey"), col("encryptedText"), col("plainText")).show(truncate = false)
  //  }
}
