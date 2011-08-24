package com.sap.security;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 4/29/11
 * Time: 10:03 AM
 * To change this template use File | Settings | File Templates.
 */
public interface IEncrypter {
    public byte[] encrypt(String plainText) throws Exception;
}
