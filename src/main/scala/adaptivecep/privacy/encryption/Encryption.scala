package adaptivecep.privacy.encryption

import java.math.BigInteger
import java.nio.ByteBuffer

import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

import scala.util.Random

/***
  * abstract encryption wrapper
  */
trait Encryption {
  def encrypt(value: Array[Byte]): Array[Byte]
  def decrypt(value: Array[Byte]): Array[Byte]

  def encryptInt(value: Int): Array[Byte]
  def decryptInt(value: Array[Byte]):Int

  def encryptString(value: String): Array[Byte]
  def decryptString(value: Array[Byte]): String

  def encryptFloat(value: Float): Array[Byte]
  def decryptFloat(value: Array[Byte]): Float

  def encryptDouble(value: Double): Array[Byte]
  def decryptDouble(value: Array[Byte]): Double

  def encryptShort(value: Short): Array[Byte]
  def decryptShort(value: Array[Byte]): Short

  def encryptChar(value: Char): Array[Byte]
  def decryptChar(value: Array[Byte]): Char

  def encryptBigInt(value: BigInt): Array[Byte]
  def decryptBigInt(value: Array[Byte]): BigInt

  def encryptBoolean(value: Boolean): Array[Byte]
  def decryptBoolean(value: Array[Byte]): Boolean

}

/***
  * this class encrypts and decrypts data based on AES encryption scheme
  * without only wrapping around how to encrypt/decrypt
  * and takes the keys and initialization vector as parameters
  * @param skeySpec
  * @param iv
  */
case class CryptoAES( skeySpec: SecretKeySpec, iv: IvParameterSpec) extends Encryption {
  override def encrypt(value: Array[Byte]): Array[Byte] = {
    val cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING")
    cipher.init(Cipher.ENCRYPT_MODE, skeySpec,iv)
    cipher.doFinal(value)
  }

  override def decrypt(value: Array[Byte]): Array[Byte] = {
    val cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING")
    cipher.init(Cipher.DECRYPT_MODE, skeySpec,iv)
    cipher.doFinal(value)
  }

  override def encryptInt(value: Int): Array[Byte] = {
    val buffer = ByteBuffer.allocate(4)
    encrypt(buffer.putInt(value).array())
  }

  override def decryptInt(value: Array[Byte]): Int = {
    val result = decrypt(value)
    ByteBuffer.wrap(result).getInt
  }

  override def encryptString(value: String): Array[Byte] = {
    encrypt(value.getBytes)
  }

  override def decryptString(value: Array[Byte]): String = {
    val result = decrypt(value)
    result.mkString
  }

  override def encryptFloat(value: Float): Array[Byte] = {
    val buffer = ByteBuffer.allocate(4)
    encrypt(buffer.putFloat(value).array())
  }

  override def decryptFloat(value: Array[Byte]): Float = {
    val result = decrypt(value)
    ByteBuffer.wrap(result).getFloat
  }

  override def encryptDouble(value: Double): Array[Byte] = {
    val buffer = ByteBuffer.allocate(8)
    encrypt(buffer.putDouble(value).array())
  }

  override def decryptDouble(value: Array[Byte]): Double = {
    val result = decrypt(value)
    ByteBuffer.wrap(result).getDouble
  }

  override def encryptShort(value: Short): Array[Byte] = {
    val buffer = ByteBuffer.allocate(2)
     encrypt(buffer.putShort(value).array())
  }

  override def decryptShort(value: Array[Byte]): Short = {
    val result = decrypt(value)
    ByteBuffer.wrap(result).getShort
  }

  override def encryptChar(value: Char): Array[Byte] = {
    val buffer = ByteBuffer.allocate(2)
    encrypt(buffer.putChar(value).array())
  }

  override def decryptChar(value: Array[Byte]): Char = {
    val result = decrypt(value)
    ByteBuffer.wrap(result).getChar
  }

  override def encryptBigInt(value: BigInt): Array[Byte] = {
    encrypt(value.toByteArray)
  }

  override def decryptBigInt(value: Array[Byte]): BigInt = {
    val result = decrypt(value)
    BigInt(result)
  }

  override def encryptBoolean(value: Boolean): Array[Byte] = {
    val randomInt = Random.nextInt(Integer.MAX_VALUE)
    val buffer = ByteBuffer.allocate(4)
    if(value){
      val posInt = buffer.putInt(randomInt).array()
      encrypt(posInt)
    }else{
      val negInt = buffer.putInt(-1 * randomInt ).array()
      encrypt(negInt)
    }
  }

  override def decryptBoolean(value: Array[Byte]): Boolean = {
    val result = decrypt(value)
    val intVal = ByteBuffer.wrap(result).getInt
    if (intVal > 0) true else false
  }
}