package com.hainiu.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

public class OrcFormat {
    public static String TYPE;

    private static final String STRUCTURE_READER_PATH = "/OrcReaderStructure.data";


    static {
        try (   BufferedReader b_r = new BufferedReader(new InputStreamReader(OrcFormat.class.getResourceAsStream(STRUCTURE_READER_PATH)))
        ){
            TYPE = b_r.readLine();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
