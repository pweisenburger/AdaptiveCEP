package adaptivecep.privacy.encryption

import java.math.BigInteger

import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

trait Encryption {
  def encrypt(value: Array[Byte]): Array[Byte]
  def decrypt(value: Array[Byte]): Array[Byte]
}

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

//object AES extends CryptoAES
