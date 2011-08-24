package com.sap.hadoop.sort;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Jan 20, 2011
 * Time: 4:22:29 PM
 * To change this template use File | Settings | File Templates.
 */
public class RandomNumberGenerator {
    private static String BASE_FILENAME = "c:\\temp\\numbersToSort";

    public static void main(String[] arg) throws Exception {
        int oneMillion = 1000000;
        int numberPerFile = 1000 * oneMillion;
        int numberOfFiles = 1;
        int writeFreq = oneMillion;

        int fileCount = 0;
        int numberCount = 0;
        List<Integer> numberInFile = new ArrayList<Integer>(numberPerFile);
        String filename = BASE_FILENAME + "_" + getPaddedString(fileCount, 6, '0');

        while (true) {
            int number = (int) (Math.random() * 1000);
            numberInFile.add(number);
            numberCount++;
/*
if (numberInFile.size() > 0 && numberInFile.size() % oneMillion == 0){
System.out.println("numberInFile.size()="+numberInFile.size());
}
*/
            if (numberCount % writeFreq == 0) {
                Utility.writeTo(filename + ".txt", getLine(numberInFile, ',', true), true);
                numberInFile = new ArrayList<Integer>();
            }

            if (numberCount >= numberPerFile) {
                fileCount++;
                if (fileCount > numberOfFiles - 1) {
                    break;
                }
                Utility.writeTo(filename + ".txt", getLine(numberInFile, ',', false), true);
                filename = BASE_FILENAME + "_" + getPaddedString(fileCount, 6, '0');
                numberInFile = new ArrayList<Integer>();
                numberCount = 0;
            }

        }
        System.exit(0);
    }

    private static String getPaddedString(int num, int width, char paddedChar) {
        String strNumber = String.valueOf(num);
        StringBuilder sb = new StringBuilder();
        int paddedLength = width - strNumber.length();
        if (paddedLength > 0) {
            for (int i = 0; i < paddedLength; i++) {
                sb.append(paddedChar);
            }
        }
        return sb.append(strNumber).toString();
    }

    private static String getLine(Collection<Integer> numbers, char delimiter, boolean addDelimiter) {
        StringBuilder sb = new StringBuilder();
        for (int number : numbers) {
            sb.append(String.valueOf(number)).append(delimiter);
        }
        if (!addDelimiter && sb.length() > 0 && sb.charAt(sb.length() - 1) == delimiter) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

}
