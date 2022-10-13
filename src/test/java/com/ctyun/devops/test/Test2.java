package com.ctyun.devops.test;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

public class Test2 {

    @Test
    public void test1() {
        String s = "<font color='red'>北极星</font>-update";
        System.out.println(StringUtils.substringBetween(s, ">", "<"));

        s = "这是<font color='red'>研发</font>二部的描述，<font color='red'>研发</font>二部有2000人，在成都、北京、广州都有办公区";
        System.out.println(StringUtils.substringBetween(s, ">", "<"));

    }

    @Test
    public void test2() {
        StringBuilder sb = new StringBuilder();
        sb.append("asd").append("/").append("asdas").append("/");

        if (sb.length() > 0 && sb.lastIndexOf("/") == sb.length() - 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        System.out.println(sb);
    }

    @Test
    public void test3() {
        String s = "MTIzPGFzPmUyMT8nIictLT09KSgxMjIxJg==";
        System.out.println(new String(Base64.getDecoder().decode(s.getBytes(StandardCharsets.UTF_8))));

//        String s = "s(<R4u1\\>8";
//        System.out.println(Base64.getEncoder().encodeToString(s.getBytes()));
    }

    @Test
    public void test4() {
        String pwd = "s(<R4u1\\N8";

        String result = StringEscapeUtils.escapeHtml4(pwd);
        System.out.println(result);
//        String result = decodePassword(pwd);
//        System.out.println(result);
    }

    private String decodePassword(String initialPassword) {
        // 替换 html 特殊标签
        return initialPassword.replace("<", "&lt;").replace(">", "&gt;");
    }


    @Test
    public void test5() {
        Boolean b = true;
        System.out.println(BooleanUtils.isNotTrue(b));
    }
}
