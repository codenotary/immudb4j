package io.codenotary.immudb4j;

public class Utils {


    public static int countBits(int number) {

        if (number == 0) {
            return 0;
        }
        // log function in base 2 & take only integer part.
        return (int) (Math.log(number) / Math.log(2) + 1);
    }

    public static boolean haveSameEntries(byte[] a, byte[] b) {

        if (a == null || b == null || a.length != b.length) {
            return false;
        }
        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    private final static char[] HEX = new char[] {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            'A', 'B', 'C', 'D', 'E', 'F'};

    /** Convert the bytes to a base 16 string. */
    public static String convertBase16(byte[] byteArray) {
        StringBuffer hexBuffer = new StringBuffer(byteArray.length * 2);
        for (int i = 0; i < byteArray.length; i++)
            for (int j = 1; j >= 0; j--)
                hexBuffer.append(HEX[(byteArray[i] >> (j * 4)) & 0xF]);
        return hexBuffer.toString();
    }

}
