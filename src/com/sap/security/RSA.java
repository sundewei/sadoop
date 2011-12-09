package com.sap.security;

import javax.crypto.Cipher;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
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
public class RSA implements ISecurityTool {
    public byte[] publicKey;
    public byte[] privateKey;

    public RSA() throws NoSuchAlgorithmException, InvalidKeySpecException, IOException {
        ByteArrayOutputStream pubOut = new ByteArrayOutputStream();
        ByteArrayOutputStream priOut = new ByteArrayOutputStream();
        init(pubOut, priOut);
        publicKey = pubOut.toByteArray();
        privateKey = priOut.toByteArray();
    }

    private void init(OutputStream pubOut, OutputStream priOut) throws NoSuchAlgorithmException, InvalidKeySpecException, IOException {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(2048);
        KeyPair keyPair = keyPairGenerator.genKeyPair();
        save(keyPair, pubOut, priOut);
    }

    private void save(KeyPair keyPair, OutputStream pubOut, OutputStream priOut) throws NoSuchAlgorithmException, InvalidKeySpecException, IOException {
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        RSAPublicKeySpec publicKeySpec = keyFactory.getKeySpec(keyPair.getPublic(), RSAPublicKeySpec.class);
        RSAPrivateKeySpec privateKeySpec = keyFactory.getKeySpec(keyPair.getPrivate(), RSAPrivateKeySpec.class);
        saveKeys(pubOut, publicKeySpec.getModulus(), publicKeySpec.getPublicExponent());
        saveKeys(priOut, privateKeySpec.getModulus(), privateKeySpec.getPrivateExponent());
    }

    private void saveKeys(OutputStream out, BigInteger mod, BigInteger exp) throws IOException {
        ObjectOutputStream oout = new ObjectOutputStream(new BufferedOutputStream(out));
        try {
            oout.writeObject(mod);
            oout.writeObject(exp);
        } catch (Exception e) {
            throw new IOException("Unexpected error", e);
        } finally {
            out.close();
            oout.close();
        }
    }

    PublicKey getPublicKey(InputStream in) throws IOException {
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

    PrivateKey getPrivateKey(InputStream in) throws IOException {
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

    byte[] rsaEncrypt(byte[] data, byte[] myPrivateKey) throws Exception {
        PrivateKey privKey = getPrivateKey(new ByteArrayInputStream(myPrivateKey));
        Cipher cipher = Cipher.getInstance("RSA");
        cipher.init(Cipher.ENCRYPT_MODE, privKey);
        byte[] cipherData = cipher.doFinal(data);
        return cipherData;
    }

    byte[] rsaDecrypt(byte[] data, byte[] hisPublicKey) throws Exception {
        PublicKey pubKey = getPublicKey(new ByteArrayInputStream(hisPublicKey));
        Cipher cipher = Cipher.getInstance("RSA");
        cipher.init(Cipher.DECRYPT_MODE, pubKey);
        byte[] cipherData = cipher.doFinal(data);
        return cipherData;
    }

    public byte[] encrypt(String plainText) throws Exception {
        return rsaEncrypt(plainText.getBytes(), privateKey);
    }

    public static void main(String[] arg) throws Exception {
        String plainText = "MyPassword, Don't tell anyone...!";
        RSA rsa = new RSA();
        byte[] encryptedText = rsa.rsaEncrypt(plainText.getBytes(), rsa.privateKey);
        byte[] decryptedText = rsa.rsaDecrypt(encryptedText, rsa.publicKey);
        System.out.println(new String(decryptedText));
    }

}
