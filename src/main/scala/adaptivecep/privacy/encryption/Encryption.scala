package adaptivecep.privacy.encryption

import java.math.BigInteger

import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

/***
  * abstract encryption wrapper
  */
trait Encryption {
  def encrypt(value: Array[Byte]): Array[Byte]
  def decrypt(value: Array[Byte]): Array[Byte]
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
}