package com.hainiu.qiaoChunYu.inputCompress;

public class CompressFileInput {


    public static void main(String[] args) {
        String idCountry = args[0];

        String[] arr = idCountry.split("&");

        String id = arr[0].split("=")[1];
        String country = arr[1].split("=")[1];

    }
}
