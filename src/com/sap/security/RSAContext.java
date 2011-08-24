package com.sap.security;

import javax.crypto.Cipher;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPrivateKeySpec;
import java.security.spec.RSAPublicKeySpec;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 4/29/11
 * Time: 10:06 AM
 * To change this template use File | Settings | File Templates.
 */
public class RSAContext implements IEncrypter {
    private static String publicKeyFile = "public.key";
    private static String privateKeyFile = "private.key";

    private static final RSAContext STATIC_INSTANCE = new RSAContext();

    private RSAContext() {
    }

    public static RSAContext getInstance() {
        return STATIC_INSTANCE;
    }

    private static void init() throws NoSuchAlgorithmException, InvalidKeySpecException, IOException {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(2048);
        KeyPair keyPair = keyPairGenerator.genKeyPair();
        save(keyPair);
    }

    private static void save(KeyPair keyPair) throws NoSuchAlgorithmException, InvalidKeySpecException, IOException {
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        RSAPublicKeySpec publicKeySpec = keyFactory.getKeySpec(keyPair.getPublic(), RSAPublicKeySpec.class);
        RSAPrivateKeySpec privateKeySpec = keyFactory.getKeySpec(keyPair.getPrivate(), RSAPrivateKeySpec.class);
        saveToFile(publicKeyFile, publicKeySpec.getModulus(), publicKeySpec.getPublicExponent());
        saveToFile(privateKeyFile, privateKeySpec.getModulus(), privateKeySpec.getPrivateExponent());
    }

    private static void saveToFile(String fileName, BigInteger mod, BigInteger exp) throws IOException {
        ObjectOutputStream oout = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(fileName)));
        try {
            oout.writeObject(mod);
            oout.writeObject(exp);
        } catch (Exception e) {
            throw new IOException("Unexpected error", e);
        } finally {
            oout.close();
        }
    }

    static PublicKey getPublicKey(String keyFileName) throws IOException {
        InputStream in = RSAContext.class.getResourceAsStream(keyFileName);
        ObjectInputStream oin = new ObjectInputStream(new BufferedInputStream(in));
        try {
            BigInteger m = (BigInteger) oin.readObject();
            BigInteger e = (BigInteger) oin.readObject();
            RSAPublicKeySpec keySpec = new RSAPublicKeySpec(m, e);
            KeyFactory fact = KeyFactory.getInstance("RSA");
            return fact.generatePublic(keySpec);
        } catch (Exception e) {
            throw new RuntimeException("Public Key: spurious serialisation error", e);
        } finally {
            oin.close();
        }
    }

    static PrivateKey getPrivateKey(String keyFileName) throws IOException {
        InputStream in = RSAContext.class.getResource(keyFileName).openStream();
        ObjectInputStream oin = new ObjectInputStream(new BufferedInputStream(in));
        try {
            BigInteger m = (BigInteger) oin.readObject();
            BigInteger e = (BigInteger) oin.readObject();
            RSAPrivateKeySpec keySpec = new RSAPrivateKeySpec(m, e);
            KeyFactory fact = KeyFactory.getInstance("RSA");
            return fact.generatePrivate(keySpec);
        } catch (Exception e) {
            throw new RuntimeException("Private Key: spurious serialisation error", e);
        } finally {
            oin.close();
        }
    }

    static byte[] rsaEncrypt(byte[] data) throws Exception {
        PublicKey pubKey = getPublicKey(publicKeyFile);
        Cipher cipher = Cipher.getInstance("RSA");
        cipher.init(Cipher.ENCRYPT_MODE, pubKey);
        byte[] cipherData = cipher.doFinal(data);
        return cipherData;
    }

    static byte[] rsaDecrypt(byte[] data) throws Exception {
        PrivateKey privKey = getPrivateKey(privateKeyFile);
        Cipher cipher = Cipher.getInstance("RSA");
        cipher.init(Cipher.DECRYPT_MODE, privKey);
        byte[] cipherData = cipher.doFinal(data);
        return cipherData;
    }

    public byte[] encrypt(String plainText) throws Exception {
        return rsaEncrypt(plainText.getBytes());
    }
    /*
    public static void main(String[] arg) throws Exception {
        String plainText = "MyPassword, Don't tell anyone...!";
        byte[] encryptedText = rsaEncrypt(plainText.getBytes());
        byte[] decryptedText = rsaDecrypt(encryptedText);
    }
    */
}
