package com.ctyun.devops.utils;

import org.apache.shiro.crypto.hash.ConfigurableHashService;
import org.apache.shiro.crypto.hash.DefaultHashService;
import org.apache.shiro.crypto.hash.HashRequest;
import org.apache.shiro.util.ByteSource;

import java.util.Base64;

public class Encryption {

    private String staticSalt = ".";
    private String algorithmName = "MD5";

    /**
     * 使用默认加密算法进行编码
     */
    public String encode(String username, String rawPassword) {
        ConfigurableHashService hashService = new DefaultHashService();
        hashService.setPrivateSalt(ByteSource.Util.bytes(this.staticSalt));
        hashService.setHashAlgorithmName(this.algorithmName);
        hashService.setHashIterations(2);
        HashRequest request = new HashRequest.Builder()
                .setSalt(username)
                .setSource(rawPassword)
                .build();
        return hashService.computeHash(request).toHex();
    }

    public static void main(String[] args) {
        String codeInitPwd= "Y0gqbDhUXDRFVA==";
        String initPwd = new String(Base64.getDecoder().decode(codeInitPwd.getBytes()));
        System.out.println(initPwd);

        String username = "chengchao1@chinatelecom.cn";
        System.out.println(new Encryption().encode(username, initPwd));
    }

}
